package test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"gogo/utils"
)

func TestScheduler_AddAndRemoveJob(t *testing.T) {
	scheduler := utils.NewScheduler()
	scheduler.Start()
	defer scheduler.Stop()

	var runCount int32

	err := scheduler.AddJob("job1", "*/1 * * * * *", func() {
		atomic.AddInt32(&runCount, 1)
	})
	assert.NoError(t, err)

	// 等待任务执行几次
	time.Sleep(2500 * time.Millisecond)

	// 检查任务执行次数
	assert.GreaterOrEqual(t, runCount, int32(2))

	// 删除任务
	err = scheduler.RemoveJob("job1")
	assert.NoError(t, err)

	// 等待确认任务已被删除（不再执行）
	current := atomic.LoadInt32(&runCount)
	time.Sleep(2 * time.Second)
	assert.Equal(t, current, atomic.LoadInt32(&runCount))
}

func TestScheduler_DuplicateJob(t *testing.T) {
	scheduler := utils.NewScheduler()
	scheduler.Start()
	defer scheduler.Stop()

	err := scheduler.AddJob("duplicate", "*/2 * * * * *", func() {})
	assert.NoError(t, err)

	err = scheduler.AddJob("duplicate", "*/3 * * * * *", func() {})
	assert.Error(t, err)
	assert.Equal(t, "job ID already exists", err.Error())
}

func TestScheduler_RemoveNonExistentJob(t *testing.T) {
	scheduler := utils.NewScheduler()
	scheduler.Start()
	defer scheduler.Stop()

	err := scheduler.RemoveJob("non-existent")
	assert.Error(t, err)
	assert.Equal(t, "job ID not found", err.Error())
}

func TestScheduler_HasJob(t *testing.T) {
	scheduler := utils.NewScheduler()
	scheduler.Start()
	defer scheduler.Stop()

	assert.False(t, scheduler.HasJob("check"))

	err := scheduler.AddJob("check", "*/1 * * * * *", func() {})
	assert.NoError(t, err)

	assert.True(t, scheduler.HasJob("check"))

	err = scheduler.RemoveJob("check")
	assert.NoError(t, err)

	assert.False(t, scheduler.HasJob("check"))
}
