package internal

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/internal/common/metrics"
	ilog "go.temporal.io/sdk/internal/log"
	"go.temporal.io/sdk/log"
)

type fakeReserveInfo struct {
	issued int
}

func (f *fakeReserveInfo) TaskQueue() string { return "test-tq" }
func (f *fakeReserveInfo) TaskQueueKind() enumspb.TaskQueueKind {
	return enumspb.TASK_QUEUE_KIND_NORMAL
}
func (f *fakeReserveInfo) WorkerBuildId() string           { return "" }
func (f *fakeReserveInfo) WorkerIdentity() string          { return "" }
func (f *fakeReserveInfo) NumIssuedSlots() int             { return f.issued }
func (f *fakeReserveInfo) Logger() log.Logger              { return ilog.NewNopLogger() }
func (f *fakeReserveInfo) MetricsHandler() metrics.Handler { return metrics.NopHandler }

// TestRampThrottleBoundsIssuanceAcrossConcurrentReserveSlot pins down the throttle invariant when
// several ReserveSlot calls contend at once: past MinSlots, successive slots are issued no closer
// together than RampThrottle.
//
// The interesting case is when every caller independently decides it does not need to wait, which
// happens whenever the last slot was issued longer than RampThrottle ago. What bounds issuance then
// is TryReserveSlot: it reads lastSlotIssuedAt, consults the controller, and stamps lastSlotIssuedAt
// all under lastIssuedMu, so of two racing callers the second sees the first's stamp and is denied.
func TestRampThrottleBoundsIssuanceAcrossConcurrentReserveSlot(t *testing.T) {
	const ramp = 100 * time.Millisecond
	const reservers = 4

	rcOpts := DefaultResourceControllerOptions()
	rcOpts.MemTargetPercent = 0.8
	rcOpts.CpuTargetPercent = 0.9
	// Usage well under target, so the controller always says yes and RampThrottle is the only gate.
	rcOpts.InfoSupplier = &FakeSystemInfoSupplier{memUse: 0.1, cpuUse: 0.1}

	supplier, err := NewResourceBasedSlotSupplier(NewResourceController(rcOpts),
		ResourceBasedSlotSupplierOptions{MinSlots: 0, MaxSlots: 1000, RampThrottle: ramp})
	require.NoError(t, err)

	// Issued count is past MinSlots so the throttle applies, and lastSlotIssuedAt is still the zero
	// time, so every goroutine computes a non-positive wait and goes straight for a slot.
	info := &fakeReserveInfo{issued: 10}

	var mu sync.Mutex
	issuedAt := make([]time.Time, 0, reservers)
	var reserveErrs []error

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < reservers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			permit, err := supplier.ReserveSlot(context.Background(), info)
			at := time.Now()

			mu.Lock()
			defer mu.Unlock()
			if err != nil || permit == nil {
				reserveErrs = append(reserveErrs, err)
				return
			}
			issuedAt = append(issuedAt, at)
		}()
	}
	close(start)
	wg.Wait()

	require.Empty(t, reserveErrs)
	require.Len(t, issuedAt, reservers)

	sort.Slice(issuedAt, func(i, j int) bool { return issuedAt[i].Before(issuedAt[j]) })
	for i := 1; i < len(issuedAt); i++ {
		gap := issuedAt[i].Sub(issuedAt[i-1])
		t.Logf("slots %d and %d issued %v apart", i-1, i, gap)
		// Half of RampThrottle leaves room for scheduling noise while still catching the failure
		// mode this guards against, where two callers issue within microseconds of each other.
		require.Greater(t, gap, ramp/2, "RampThrottle of %v was not applied between slots %d and %d",
			ramp, i-1, i)
	}
}
