// planner.go
package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// MigrationPlan - структура для хранения плана миграции
type MigrationPlan struct {
	ID           string               `json:"id"`
	CreatedAt    time.Time            `json:"createdAt"`
	Databases    []string             `json:"databases"`
	Strategy     string               `json:"strategy"` // "parallel", "sequential", "custom"
	Steps        []MigrationPlanStep  `json:"steps"`
	Dependencies map[string][]string  `json:"dependencies"` // зависимости между БД
	Options      MigrationPlanOptions `json:"options"`
	Status       string               `json:"status"` // "pending", "running", "completed", "failed", "cancelled"
	StartTime    *time.Time           `json:"startTime,omitempty"`
	EndTime      *time.Time           `json:"endTime,omitempty"`
	Error        error                `json:"error,omitempty"`
}

// MigrationPlanStep - шаг в плане миграции
type MigrationPlanStep struct {
	ID           string    `json:"id"`
	DBName       string    `json:"dbName"`
	Action       string    `json:"action"` // "prepare", "configure", "migrate", "replicate", "grant", "cutover"
	Order        int       `json:"order"`
	Parallel     bool      `json:"parallel"`
	Dependencies []string  `json:"dependencies"`
	Status       string    `json:"status"` // "pending", "running", "completed", "failed"
	StartTime    time.Time `json:"startTime,omitempty"`
	EndTime      time.Time `json:"endTime,omitempty"`
	Error        error     `json:"error,omitempty"`
}

// MigrationPlanOptions - опции планирования
type MigrationPlanOptions struct {
	ParallelMigrations bool       `json:"parallelMigrations"`
	MaxConcurrent      int        `json:"maxConcurrent"`
	RollbackOnFailure  bool       `json:"rollbackOnFailure"`
	DryRun             bool       `json:"dryRun"`
	WindowStart        *time.Time `json:"windowStart,omitempty"`
	WindowEnd          *time.Time `json:"windowEnd,omitempty"`
}

// PlanStatus - статус выполнения плана
type PlanStatus struct {
	ID          string                     `json:"id"`
	Status      string                     `json:"status"`
	Progress    float64                    `json:"progress"`
	Steps       map[string]*PlanStepStatus `json:"steps"`
	Completed   int                        `json:"completed"`
	Total       int                        `json:"total"`
	Error       error                      `json:"error,omitempty"`
	CurrentStep *MigrationPlanStep         `json:"currentStep,omitempty"`
}

// PlanStepStatus - статус шага плана
type PlanStepStatus struct {
	ID       string        `json:"id"`
	Status   string        `json:"status"`
	Progress float64       `json:"progress"`
	Error    error         `json:"error,omitempty"`
	Duration time.Duration `json:"duration,omitempty"`
}

// PlanChanges - изменения в плане
type PlanChanges struct {
	AddSteps      []MigrationPlanStep   `json:"addSteps"`
	RemoveSteps   []string              `json:"removeSteps"`
	UpdateSteps   []MigrationPlanStep   `json:"updateSteps"`
	UpdateOptions *MigrationPlanOptions `json:"updateOptions"`
}

// MigrationPlanner - интерфейс для планирования миграций
type MigrationPlanner interface {
	CreatePlan(databases []string, options MigrationPlanOptions) (*MigrationPlan, error)
	ValidatePlan(plan *MigrationPlan) error
	ExecutePlan(plan *MigrationPlan) error
	UpdatePlan(plan *MigrationPlan, changes PlanChanges) (*MigrationPlan, error)
	GetPlanStatus(planID string) (*PlanStatus, error)
}

// DefaultMigrationPlanner - реализация планировщика по умолчанию
type DefaultMigrationPlanner struct {
	config AppConfig
	values *ValuesFile
	logger *logrus.Logger
}

// NewDefaultMigrationPlanner создает новый экземпляр планировщика
func NewDefaultMigrationPlanner(config AppConfig, values *ValuesFile) *DefaultMigrationPlanner {
	return &DefaultMigrationPlanner{
		config: config,
		values: values,
		logger: Logger,
	}
}

// CreatePlan создает новый план миграции
func (p *DefaultMigrationPlanner) CreatePlan(databases []string, options MigrationPlanOptions) (*MigrationPlan, error) {
	plan := &MigrationPlan{
		ID:           fmt.Sprintf("plan_%d", time.Now().Unix()),
		CreatedAt:    time.Now(),
		Databases:    databases,
		Strategy:     "sequential", // по умолчанию
		Steps:        make([]MigrationPlanStep, 0),
		Dependencies: make(map[string][]string),
		Options:      options,
		Status:       "pending",
	}

	// Устанавливаем стратегию в зависимости от опций
	if options.ParallelMigrations {
		plan.Strategy = "parallel"
	} else {
		plan.Strategy = "sequential"
	}

	// Создаем шаги в зависимости от стратегии
	if err := p.generatePlanSteps(plan); err != nil {
		return nil, fmt.Errorf("не удалось сгенерировать шаги плана: %w", err)
	}

	p.logger.WithFields(logrus.Fields{
		"plan_id":     plan.ID,
		"databases":   databases,
		"strategy":    plan.Strategy,
		"total_steps": len(plan.Steps),
	}).Info("Migration plan created successfully")

	return plan, nil
}

// generatePlanSteps генерирует шаги для плана в зависимости от стратегии
func (p *DefaultMigrationPlanner) generatePlanSteps(plan *MigrationPlan) error {
	order := 0

	for _, dbName := range plan.Databases {
		// Проверяем, что база данных существует в конфигурации
		if _, exists := p.values.StatefulSets[dbName]; !exists {
			return fmt.Errorf("база данных %s не найдена в конфигурации", dbName)
		}

		// Создаем шаги для каждой базы данных
		steps := []MigrationPlanStep{
			{
				ID:       fmt.Sprintf("prepare_%s", dbName),
				DBName:   dbName,
				Action:   "prepare",
				Order:    order,
				Parallel: plan.Strategy == "parallel",
				Status:   "pending",
			},
			{
				ID:       fmt.Sprintf("configure_%s", dbName),
				DBName:   dbName,
				Action:   "configure",
				Order:    order + 1,
				Parallel: plan.Strategy == "parallel",
				Status:   "pending",
			},
			{
				ID:       fmt.Sprintf("migrate_%s", dbName),
				DBName:   dbName,
				Action:   "migrate",
				Order:    order + 2,
				Parallel: plan.Strategy == "parallel",
				Status:   "pending",
			},
			{
				ID:       fmt.Sprintf("replicate_%s", dbName),
				DBName:   dbName,
				Action:   "replicate",
				Order:    order + 3,
				Parallel: plan.Strategy == "parallel",
				Status:   "pending",
			},
			{
				ID:       fmt.Sprintf("grant_%s", dbName),
				DBName:   dbName,
				Action:   "grant",
				Order:    order + 4,
				Parallel: plan.Strategy == "parallel",
				Status:   "pending",
			},
			{
				ID:       fmt.Sprintf("cutover_%s", dbName),
				DBName:   dbName,
				Action:   "cutover",
				Order:    order + 5,
				Parallel: false, // финальный шаг не может быть параллельным
				Status:   "pending",
			},
		}

		plan.Steps = append(plan.Steps, steps...)
		order += 6 // увеличиваем на количество шагов для одной БД
	}

	return nil
}

// AddDependency добавляет зависимость между базами данных
func (p *DefaultMigrationPlanner) AddDependency(plan *MigrationPlan, fromDB, toDB string, action string) error {
	// Проверяем, что обе базы данных существуют в плане
	fromExists := false
	toExists := false
	for _, db := range plan.Databases {
		if db == fromDB {
			fromExists = true
		}
		if db == toDB {
			toExists = true
		}
	}

	if !fromExists || !toExists {
		return fmt.Errorf("одна или обе базы данных не существуют в плане миграции")
	}

	// Находим шаг, от которого зависит другой
	dependencyStepID := fmt.Sprintf("%s_%s", action, fromDB)
	targetStepID := fmt.Sprintf("%s_%s", action, toDB)

	// Добавляем зависимость
	for i := range plan.Steps {
		if plan.Steps[i].ID == targetStepID {
			// Проверяем, есть ли уже такая зависимость
			alreadyExists := false
			for _, dep := range plan.Steps[i].Dependencies {
				if dep == dependencyStepID {
					alreadyExists = true
					break
				}
			}
			if !alreadyExists {
				plan.Steps[i].Dependencies = append(plan.Steps[i].Dependencies, dependencyStepID)
			}
			break
		}
	}

	return nil
}

// GetPlanExecutionOrder возвращает порядок выполнения шагов с учетом зависимостей
func (p *DefaultMigrationPlanner) GetPlanExecutionOrder(plan *MigrationPlan) ([][]MigrationPlanStep, error) {
	// Создаем граф зависимостей
	graph := make(map[string][]string)
	indegree := make(map[string]int)

	// Инициализируем граф
	for _, step := range plan.Steps {
		graph[step.ID] = []string{}
		indegree[step.ID] = 0
	}

	// Заполняем зависимости
	for _, step := range plan.Steps {
		for _, dep := range step.Dependencies {
			graph[dep] = append(graph[dep], step.ID)
			indegree[step.ID]++
		}
	}

	// Находим узлы без входящих зависимостей
	queue := []string{}
	for _, step := range plan.Steps {
		if indegree[step.ID] == 0 {
			queue = append(queue, step.ID)
		}
	}

	// Выполняем топологическую сортировку
	var result [][]MigrationPlanStep
	visited := make(map[string]bool)

	for len(queue) > 0 {
		currentBatch := []MigrationPlanStep{}
		nextQueue := []string{}

		// Обрабатываем все узлы текущего уровня
		for _, nodeID := range queue {
			if visited[nodeID] {
				continue
			}

			// Находим шаг по ID
			var currentStep *MigrationPlanStep
			for _, step := range plan.Steps {
				if step.ID == nodeID {
					currentStep = &step
					break
				}
			}

			if currentStep != nil {
				currentBatch = append(currentBatch, *currentStep)
				visited[nodeID] = true

				// Уменьшаем степени зависимых узлов
				for _, neighbor := range graph[nodeID] {
					indegree[neighbor]--
					if indegree[neighbor] == 0 {
						nextQueue = append(nextQueue, neighbor)
					}
				}
			}
		}

		if len(currentBatch) > 0 {
			result = append(result, currentBatch)
		}

		queue = nextQueue
	}

	// Проверяем, все ли шаги были обработаны (отсутствие циклов)
	if len(visited) != len(plan.Steps) {
		return nil, fmt.Errorf("обнаружен цикл в зависимостях плана миграции")
	}

	return result, nil
}

// ValidatePlan валидирует план миграции
func (p *DefaultMigrationPlanner) ValidatePlan(plan *MigrationPlan) error {
	if len(plan.Databases) == 0 {
		return fmt.Errorf("план миграции должен содержать хотя бы одну базу данных")
	}

	if plan.Options.MaxConcurrent < 0 {
		return fmt.Errorf("максимальное количество одновременных миграций не может быть отрицательным")
	}

	if plan.Options.MaxConcurrent == 0 {
		plan.Options.MaxConcurrent = 1 // по умолчанию
	}

	// Проверяем, что все базы данных существуют в конфигурации
	for _, dbName := range plan.Databases {
		if _, exists := p.values.StatefulSets[dbName]; !exists {
			return fmt.Errorf("база данных %s не найдена в конфигурации", dbName)
		}
	}

	// Проверяем временные окна
	if plan.Options.WindowStart != nil && plan.Options.WindowEnd != nil {
		if plan.Options.WindowStart.After(*plan.Options.WindowEnd) {
			return fmt.Errorf("время начала окна миграции не может быть позже времени окончания")
		}
	}

	p.logger.WithField("plan_id", plan.ID).Info("Migration plan validated successfully")
	return nil
}

// GetPlanStatus возвращает статус выполнения плана
func (p *DefaultMigrationPlanner) GetPlanStatus(planID string) (*PlanStatus, error) {
	// В реальной реализации статусы планов должны храниться в каком-то хранилище
	// Для упрощения возвращаем заглушку
	return &PlanStatus{
		ID:        planID,
		Status:    "pending",
		Progress:  0.0,
		Steps:     make(map[string]*PlanStepStatus),
		Completed: 0,
		Total:     0,
	}, nil
}

// GetPlanProgress возвращает прогресс выполнения плана
func (p *DefaultMigrationPlanner) GetPlanProgress(plan *MigrationPlan) *PlanStatus {
	totalSteps := len(plan.Steps)
	completedSteps := 0
	failedSteps := 0

	status := &PlanStatus{
		ID:        plan.ID,
		Status:    plan.Status,
		Steps:     make(map[string]*PlanStepStatus),
		Total:     totalSteps,
		Completed: 0,
	}

	for _, step := range plan.Steps {
		stepStatus := &PlanStepStatus{
			ID:     step.ID,
			Status: step.Status,
		}

		if step.StartTime.IsZero() && step.EndTime.IsZero() {
			stepStatus.Progress = 0.0
		} else if !step.StartTime.IsZero() && step.EndTime.IsZero() {
			// Шаг в процессе выполнения
			stepStatus.Progress = 0.5
		} else if !step.StartTime.IsZero() && !step.EndTime.IsZero() {
			// Шаг завершен
			stepStatus.Progress = 1.0
			stepStatus.Duration = step.EndTime.Sub(step.StartTime)
		}

		if step.Error != nil {
			stepStatus.Error = step.Error
		}

		status.Steps[step.ID] = stepStatus

		if step.Status == "completed" {
			completedSteps++
		} else if step.Status == "failed" {
			failedSteps++
		}
	}

	status.Completed = completedSteps
	if totalSteps > 0 {
		status.Progress = float64(completedSteps) / float64(totalSteps)
	} else {
		status.Progress = 0.0
	}

	// Определяем статус плана на основе статусов шагов
	if plan.Status != "running" {
		status.Status = plan.Status
	} else if failedSteps > 0 {
		status.Status = "failed"
	} else if completedSteps == totalSteps {
		status.Status = "completed"
	} else {
		status.Status = "running"
	}

	// Находим текущий шаг
	for _, step := range plan.Steps {
		if step.Status == "running" {
			status.CurrentStep = &step
			break
		}
	}

	return status
}

// UpdatePlan обновляет план миграции
func (p *DefaultMigrationPlanner) UpdatePlan(plan *MigrationPlan, changes PlanChanges) (*MigrationPlan, error) {
	// Обновляем опции, если они предоставлены
	if changes.UpdateOptions != nil {
		plan.Options = *changes.UpdateOptions
	}

	// Добавляем новые шаги
	for _, step := range changes.AddSteps {
		plan.Steps = append(plan.Steps, step)
	}

	// Удаляем указанные шаги
	if len(changes.RemoveSteps) > 0 {
		newSteps := make([]MigrationPlanStep, 0)
		for _, step := range plan.Steps {
			shouldRemove := false
			for _, removeID := range changes.RemoveSteps {
				if step.ID == removeID {
					shouldRemove = true
					break
				}
			}
			if !shouldRemove {
				newSteps = append(newSteps, step)
			}
		}
		plan.Steps = newSteps
	}

	// Обновляем существующие шаги
	for _, updateStep := range changes.UpdateSteps {
		for i, step := range plan.Steps {
			if step.ID == updateStep.ID {
				plan.Steps[i] = updateStep
				break
			}
		}
	}

	p.logger.WithField("plan_id", plan.ID).Info("Migration plan updated successfully")
	return plan, nil
}

// ExecutePlan выполняет план миграции
func (p *DefaultMigrationPlanner) ExecutePlan(plan *MigrationPlan) error {
	p.logger.WithField("plan_id", plan.ID).Info("Starting execution of migration plan")

	// Проверяем, что план валиден
	if err := p.ValidatePlan(plan); err != nil {
		return fmt.Errorf("план миграции не прошел валидацию: %w", err)
	}

	// Проверяем временные окна
	if plan.Options.WindowStart != nil && time.Now().Before(*plan.Options.WindowStart) {
		p.logger.WithField("window_start", plan.Options.WindowStart).Info("Waiting for migration window start time")
		time.Sleep(time.Until(*plan.Options.WindowStart))
	}

	if plan.Options.WindowEnd != nil && time.Now().After(*plan.Options.WindowEnd) {
		return fmt.Errorf("время окончания миграции уже прошло: %v", *plan.Options.WindowEnd)
	}

	plan.Status = "running"
	startTime := time.Now()
	plan.StartTime = &startTime

	// Создаем MultiMigrator для выполнения миграции
	multiMigrator := NewMultiMigrator(p.config, p.values, plan.Options.DryRun)

	// Добавляем все базы данных в мигратор
	for _, dbName := range plan.Databases {
		multiMigrator.AddDB(dbName)
	}

	// Выполняем миграцию в зависимости от стратегии
	var err error
	if plan.Strategy == "parallel" {
		err = p.executePlanParallel(plan, multiMigrator)
	} else {
		err = p.executePlanSequential(plan, multiMigrator)
	}

	if err != nil {
		plan.Status = "failed"
		plan.Error = err
		endTime := time.Now()
		plan.EndTime = &endTime

		p.logger.WithError(err).WithField("plan_id", plan.ID).Error("Migration plan execution failed")

		// Выполняем откат, если включена опция
		if plan.Options.RollbackOnFailure {
			p.logger.WithField("plan_id", plan.ID).Info("Starting rollback due to migration failure")
			rollbackErr := multiMigrator.RollbackAll()
			if rollbackErr != nil {
				p.logger.WithError(rollbackErr).WithField("plan_id", plan.ID).Error("Rollback failed")
				return fmt.Errorf("ошибка миграции: %v, ошибка отката: %v", err, rollbackErr)
			}
			p.logger.WithField("plan_id", plan.ID).Info("Rollback completed successfully")
		}

		return err
	}

	plan.Status = "completed"
	endTime := time.Now()
	plan.EndTime = &endTime

	p.logger.WithField("plan_id", plan.ID).Info("Migration plan executed successfully")
	return nil
}

// Вспомогательные функции для executePlanSequential
func (p *DefaultMigrationPlanner) getExecutionOrder(plan *MigrationPlan) ([][]MigrationPlanStep, error) {
	executionOrder, err := p.GetPlanExecutionOrder(plan)
	if err != nil {
		return nil, fmt.Errorf("ошибка определения порядка выполнения шагов: %w", err)
	}

	return executionOrder, nil
}

func (p *DefaultMigrationPlanner) executeBatchSequentially(plan *MigrationPlan, multiMigrator *MultiMigrator, batch []MigrationPlanStep) error {
	for _, step := range batch {
		dbState, exists := multiMigrator.DBStates[step.DBName]
		if !exists {
			return fmt.Errorf("состояние для базы данных %s не найдено", step.DBName)
		}

		p.logger.WithFields(logrus.Fields{
			"plan_id": plan.ID,
			"db_name": step.DBName,
			"step_id": step.ID,
			"action":  step.Action,
		}).Info("Starting migration step")

		// Обновляем статус шага в плане
		p.updateStepStatus(plan, step.ID, "running")

		// Выполняем действие в зависимости от типа шага
		var err error
		switch step.Action {
		case "prepare":
			err = dbState.Migrator.PrepareServers()
		case "configure":
			err = dbState.Migrator.ConfigureK8sPublisher()
		case "migrate":
			err = dbState.Migrator.MigrateSchema()
		case "replicate":
			err = dbState.Migrator.StartAndVerifyReplication()
		case "grant":
			err = dbState.Migrator.GrantAppPermissions()
		case "cutover":
			err = dbState.Migrator.FinalCutover()
		default:
			err = fmt.Errorf("неизвестное действие: %s", step.Action)
		}

		if err != nil {
			p.updateStepStatus(plan, step.ID, "failed", err)
			return fmt.Errorf("ошибка выполнения шага %s для %s: %w", step.Action, step.DBName, err)
		}

		p.updateStepStatus(plan, step.ID, "completed")
		p.logger.WithFields(logrus.Fields{
			"plan_id": plan.ID,
			"db_name": step.DBName,
			"step_id": step.ID,
			"action":  step.Action,
		}).Info("Migration step completed")
	}

	return nil
}

// executePlanSequential выполняет план последовательно
func (p *DefaultMigrationPlanner) executePlanSequential(plan *MigrationPlan, multiMigrator *MultiMigrator) error {
	p.logger.WithField("plan_id", plan.ID).Info("Executing migration plan sequentially")

	// Получаем порядок выполнения шагов с учетом зависимостей
	executionOrder, err := p.getExecutionOrder(plan)
	if err != nil {
		return err
	}

	// Выполняем шаги в соответствии с порядком
	for batchIndex, batch := range executionOrder {
		p.logger.WithFields(logrus.Fields{
			"plan_id":     plan.ID,
			"batch_index": batchIndex,
			"batch_size":  len(batch),
		}).Info("Executing batch of migration steps")

		if err := p.executeBatchSequentially(plan, multiMigrator, batch); err != nil {
			return err
		}
	}

	return nil
}

// Вспомогательные функции для executePlanParallel
func (p *DefaultMigrationPlanner) executeBatchInParallel(plan *MigrationPlan, multiMigrator *MultiMigrator, batch []MigrationPlanStep) error {
	// Ограничиваем количество одновременных миграций
	maxConcurrent := plan.Options.MaxConcurrent
	if maxConcurrent <= 0 {
		maxConcurrent = len(plan.Databases) * 6 // по умолчанию ограничиваем по общему числу шагов
	}

	// Создаем каналы для управления параллельными миграциями
	semaphore := make(chan struct{}, maxConcurrent)
	errChan := make(chan error, len(batch))
	var wg sync.WaitGroup

	// Выполняем шаги в соответствии с порядком
	for _, step := range batch {
		wg.Add(1)
		go func(step MigrationPlanStep) {
			defer wg.Done()

			// Ограничиваем количество одновременных операций
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			dbState, exists := multiMigrator.DBStates[step.DBName]
			if !exists {
				errChan <- fmt.Errorf("состояние для базы данных %s не найдено", step.DBName)
				return
			}

			p.logger.WithFields(logrus.Fields{
				"plan_id": plan.ID,
				"db_name": step.DBName,
				"step_id": step.ID,
				"action":  step.Action,
			}).Info("Starting migration step")

			// Обновляем статус шага в плане
			p.updateStepStatus(plan, step.ID, "running")

			// Выполняем действие в зависимости от типа шага
			var err error
			switch step.Action {
			case "prepare":
				err = dbState.Migrator.PrepareServers()
			case "configure":
				err = dbState.Migrator.ConfigureK8sPublisher()
			case "migrate":
				err = dbState.Migrator.MigrateSchema()
			case "replicate":
				err = dbState.Migrator.StartAndVerifyReplication()
			case "grant":
				err = dbState.Migrator.GrantAppPermissions()
			case "cutover":
				err = dbState.Migrator.FinalCutover()
			default:
				err = fmt.Errorf("неизвестное действие: %s", step.Action)
			}

			if err != nil {
				p.updateStepStatus(plan, step.ID, "failed", err)
				errChan <- fmt.Errorf("ошибка выполнения шага %s для %s: %w", step.Action, step.DBName, err)
				return
			}

			p.updateStepStatus(plan, step.ID, "completed")
			p.logger.WithFields(logrus.Fields{
				"plan_id": plan.ID,
				"db_name": step.DBName,
				"step_id": step.ID,
				"action":  step.Action,
			}).Info("Migration step completed")
		}(step)
	}

	// Ждем завершения всех шагов текущей группы
	wg.Wait()
	close(errChan)

	// Проверяем, были ли ошибки
	var errors []error
	for err := range errChan {
		if err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("ошибки при параллельной миграции: %v", errors)
	}

	return nil
}

// executePlanParallel выполняет план параллельно с учетом зависимостей
func (p *DefaultMigrationPlanner) executePlanParallel(plan *MigrationPlan, multiMigrator *MultiMigrator) error {
	p.logger.WithField("plan_id", plan.ID).Info("Executing migration plan in parallel with dependencies")

	// Получаем порядок выполнения шагов с учетом зависимостей
	executionOrder, err := p.getExecutionOrder(plan)
	if err != nil {
		return err
	}

	// Выполняем шаги в соответствии с порядком
	for batchIndex, batch := range executionOrder {
		p.logger.WithFields(logrus.Fields{
			"plan_id":     plan.ID,
			"batch_index": batchIndex,
			"batch_size":  len(batch),
		}).Info("Executing batch of migration steps in parallel")

		if err := p.executeBatchInParallel(plan, multiMigrator, batch); err != nil {
			return err
		}
	}

	return nil
}

// executeSingleDBMigration выполняет миграцию одной БД (устаревший метод, используется для обратной совместимости)
func (p *DefaultMigrationPlanner) executeSingleDBMigration(plan *MigrationPlan, migrator *Migrator) error {
	dbName := migrator.Params.DBName

	// Обновляем статус БД в плане
	p.updateStepStatus(plan, fmt.Sprintf("prepare_%s", dbName), "running")

	// Выполняем подготовку серверов
	if err := migrator.PrepareServers(); err != nil {
		p.updateStepStatus(plan, fmt.Sprintf("prepare_%s", dbName), "failed", err)
		return fmt.Errorf("ошибка подготовки серверов: %w", err)
	}
	p.updateStepStatus(plan, fmt.Sprintf("prepare_%s", dbName), "completed")

	// Обновляем статус БД в плане
	p.updateStepStatus(plan, fmt.Sprintf("configure_%s", dbName), "running")

	// Выполняем настройку публикатора
	if err := migrator.ConfigureK8sPublisher(); err != nil {
		p.updateStepStatus(plan, fmt.Sprintf("configure_%s", dbName), "failed", err)
		return fmt.Errorf("ошибка настройки публикатора: %w", err)
	}
	p.updateStepStatus(plan, fmt.Sprintf("configure_%s", dbName), "completed")

	// Обновляем статус БД в плане
	p.updateStepStatus(plan, fmt.Sprintf("migrate_%s", dbName), "running")

	// Выполняем миграцию схемы
	if err := migrator.MigrateSchema(); err != nil {
		p.updateStepStatus(plan, fmt.Sprintf("migrate_%s", dbName), "failed", err)
		return fmt.Errorf("ошибка миграции схемы: %w", err)
	}
	p.updateStepStatus(plan, fmt.Sprintf("migrate_%s", dbName), "completed")

	// Обновляем статус БД в плане
	p.updateStepStatus(plan, fmt.Sprintf("replicate_%s", dbName), "running")

	// Запускаем и проверяем репликацию
	if err := migrator.StartAndVerifyReplication(); err != nil {
		p.updateStepStatus(plan, fmt.Sprintf("replicate_%s", dbName), "failed", err)
		return fmt.Errorf("ошибка репликации: %w", err)
	}
	p.updateStepStatus(plan, fmt.Sprintf("replicate_%s", dbName), "completed")

	// Обновляем статус БД в плане
	p.updateStepStatus(plan, fmt.Sprintf("grant_%s", dbName), "running")

	// Выдаем права приложения
	if err := migrator.GrantAppPermissions(); err != nil {
		p.updateStepStatus(plan, fmt.Sprintf("grant_%s", dbName), "failed", err)
		return fmt.Errorf("ошибка выдачи прав: %w", err)
	}
	p.updateStepStatus(plan, fmt.Sprintf("grant_%s", dbName), "completed")

	// Обновляем статус БД в плане
	p.updateStepStatus(plan, fmt.Sprintf("cutover_%s", dbName), "running")

	// Выполняем финальное переключение
	if err := migrator.FinalCutover(); err != nil {
		p.updateStepStatus(plan, fmt.Sprintf("cutover_%s", dbName), "failed", err)
		return fmt.Errorf("ошибка финального переключения: %w", err)
	}
	p.updateStepStatus(plan, fmt.Sprintf("cutover_%s", dbName), "completed")

	return nil
}

// updateStepStatus обновляет статус шага в плане
func (p *DefaultMigrationPlanner) updateStepStatus(plan *MigrationPlan, stepID string, status string, errorArgs ...error) {
	for i := range plan.Steps {
		if plan.Steps[i].ID == stepID {
			plan.Steps[i].Status = status
			if status == "running" {
				plan.Steps[i].StartTime = time.Now()
			} else if status == "completed" || status == "failed" {
				plan.Steps[i].EndTime = time.Now()
			}

			if status == "failed" && len(errorArgs) > 0 {
				plan.Steps[i].Error = errorArgs[0]
			}

			p.logger.WithFields(logrus.Fields{
				"step_id": stepID,
				"status":  status,
				"plan_id": plan.ID,
			}).Info("Step status updated")

			break
		}
	}
}
