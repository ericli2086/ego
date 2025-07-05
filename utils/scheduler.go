package utils

import (
	"errors"
	"sync"

	"github.com/robfig/cron/v3"
)

type Scheduler struct {
	cron *cron.Cron
	jobs map[string]cron.EntryID
	mu   sync.Mutex
}

// NewScheduler 创建调度器（支持秒级调度）
func NewScheduler() *Scheduler {
	return &Scheduler{
		cron: cron.New(cron.WithSeconds()),
		jobs: make(map[string]cron.EntryID),
	}
}

// Start 启动调度器
func (s *Scheduler) Start() {
	s.cron.Start()
}

// Stop 停止调度器
func (s *Scheduler) Stop() {
	ctx := s.cron.Stop()
	<-ctx.Done()
}

// AddJob 添加任务
// id: 唯一任务标识符
// spec: cron 表达式
// job: 具体任务函数
func (s *Scheduler) AddJob(id string, spec string, job func()) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[id]; exists {
		return errors.New("job ID already exists")
	}

	entryID, err := s.cron.AddFunc(spec, job)
	if err != nil {
		return err
	}

	s.jobs[id] = entryID
	return nil
}

// RemoveJob 删除任务
func (s *Scheduler) RemoveJob(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entryID, exists := s.jobs[id]
	if !exists {
		return errors.New("job ID not found")
	}

	s.cron.Remove(entryID)
	delete(s.jobs, id)
	return nil
}

// HasJob 检查任务是否存在
func (s *Scheduler) HasJob(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.jobs[id]
	return exists
}
