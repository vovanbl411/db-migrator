// tui.go
package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/list"
	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/sirupsen/logrus"
)

// --- Стили ---
var (
	// Consistent padding system - using 2 units as base
	titleStyle     = lipgloss.NewStyle().PaddingLeft(2).Foreground(lipgloss.Color("170")).Bold(true)
	itemStyle      = lipgloss.NewStyle().PaddingLeft(2)
	selectedStyle  = lipgloss.NewStyle().PaddingLeft(2).Foreground(lipgloss.Color("170")).Bold(true)
	stepStyle      = lipgloss.NewStyle().PaddingLeft(2)
	finalMsgStyle  = lipgloss.NewStyle().Padding(1, 2).Align(lipgloss.Center).Bold(true)
	docStyle       = lipgloss.NewStyle().Padding(1, 2)
	helpStyle      = lipgloss.NewStyle().Foreground(lipgloss.Color("241")).PaddingLeft(2)
	borderStyle    = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color("170"))
	progressStyle  = lipgloss.NewStyle().PaddingLeft(2)
	errorStyle     = lipgloss.NewStyle().PaddingLeft(2).Foreground(lipgloss.Color("196")).Bold(true) // Ярко-красный для ошибок
	warningStyle   = lipgloss.NewStyle().PaddingLeft(2).Foreground(lipgloss.Color("220")).Bold(true)
	sectionStyle   = lipgloss.NewStyle().Border(lipgloss.NormalBorder(), false, false, true, false).BorderForeground(lipgloss.Color("240")).MarginBottom(1)
	separatorStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("240"))

	// Дополнительные стили для улучшения отображения статусов
	pendingStyle   = lipgloss.NewStyle()            // Стандартный стиль для ожидания
	runningStyle   = lipgloss.NewStyle().Bold(true) // Жирный для выполнения
	completedStyle = lipgloss.NewStyle().Bold(true) // Жирный для завершенных
	failedStyle    = lipgloss.NewStyle().Bold(true) // Жирный для ошибок
)

// --- Состояния UI ---
type viewState int

const (
	stateChoosingDB viewState = iota
	stateConfirming
	stateMigrating
	stateFinalCutoverPrompt // Новый шаг для последнего подтверждения
	stateFinished
	stateErrored
	stateViewingLogs // Состояние для просмотра логов
)

// --- Шаги миграции ---
type migrationStep struct {
	name   string
	action func() error
}

// --- Статус шага миграции ---
type stepStatus struct {
	name      string
	status    string // "pending", "running", "completed", "failed"
	startTime time.Time
	endTime   time.Time
	duration  time.Duration
	errorMsg  string
}

// --- Сообщения для асинхронной работы ---
type stepFinishedMsg struct{}
type migrationErrorMsg struct{ err error }

// --- Модель TUI ---
type model struct {
	state       viewState
	values      *ValuesFile
	migrator    *Migrator
	dbList      list.Model
	spinner     spinner.Model
	progressBar progress.Model

	steps       []migrationStep
	stepStatus  []stepStatus
	currentStep int
	err         error
	isDryRun    bool

	// Добавляем поля для поддержки миграции нескольких БД
	selectedDBs   []string
	multiMigrator *MultiMigrator

	// Добавляем поле для планировщика
	planner *DefaultMigrationPlanner

	// Добавляем поле для просмотра логов
	logViewport viewport.Model
}

// item - это элемент в нашем списке выбора БД
type item struct {
	name     string
	selected bool
}

func (i item) FilterValue() string { return "" }

type itemDelegate struct{}

func (d itemDelegate) Height() int                               { return 1 }
func (d itemDelegate) Spacing() int                              { return 0 }
func (d itemDelegate) Update(msg tea.Msg, m *list.Model) tea.Cmd { return nil }
func (d itemDelegate) Render(w io.Writer, m list.Model, index int, listItem list.Item) {
	i, ok := listItem.(item)
	if !ok {
		return
	}

	indicator := "[ ]" // не выбран (ASCII символы)
	if i.selected {
		indicator = "[x]" // выбран (ASCII символы)
	}

	str := fmt.Sprintf("%d. [%s] %s", index+1, indicator, i.name)

	if index == m.Index() {
		// Если элемент выбран, используем selectedStyle
		fmt.Fprint(w, selectedStyle.Render("> "+str))
	} else {
		// Иначе, используем обычный itemStyle
		fmt.Fprint(w, itemStyle.Render(str))
	}
}

// NewTUIModel - конструктор
func NewTUIModel(config AppConfig, values *ValuesFile, isDryRun bool) model {
	Logger.WithFields(logrus.Fields{
		"is_dry_run": isDryRun,
		"db_count":   len(values.StatefulSets),
	}).Info("Initializing TUI model")

	var dbItems []list.Item
	var dbNames []string
	for name, statefulset := range values.StatefulSets {
		// Проверяем, что это postgres база данных
		if strings.HasPrefix(name, "postgres-") {
			// Проверяем, что конфигурация базы данных не пуста
			if statefulset.ConfigMap.DB != "" {
				dbNames = append(dbNames, name)
			}
		}
	}
	sort.Strings(dbNames)
	for _, name := range dbNames {
		dbItems = append(dbItems, item{name: name, selected: false})
	}

	l := list.New(dbItems, itemDelegate{}, 20, 14)
	l.Title = "Выберите базы данных для миграции (пробел для выбора, Enter для продолжения)"
	l.SetShowStatusBar(false)
	l.SetFilteringEnabled(false)
	l.Styles.Title = titleStyle
	l.Styles.HelpStyle = helpStyle

	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("205"))

	// Создаем наш прогресс-бар
	prog := progress.New(progress.WithDefaultGradient())

	migrator := NewMigrator(config)
	migrator.DryRun = isDryRun

	// Создаем планировщик
	planner := NewDefaultMigrationPlanner(config, values)

	Logger.Info("TUI model initialized successfully")
	return model{
		state:         stateChoosingDB,
		values:        values,
		dbList:        l,
		spinner:       s,
		migrator:      migrator,
		isDryRun:      isDryRun,
		progressBar:   prog,
		stepStatus:    make([]stepStatus, 0),
		multiMigrator: NewMultiMigrator(config, values, isDryRun),
		planner:       planner,
		logViewport:   viewport.New(80, 20), // Инициализируем viewport с начальными размерами
	}
}

// Init - первая команда при запуске
func (m model) Init() tea.Cmd {
	// Инициализируем команду спиннера
	cmd := m.spinner.Tick

	// Возвращаем команду инициализации
	return cmd
}

// Вспомогательные функции для Update

func (m model) updateChoosingDBState(msg tea.Msg) (tea.Model, tea.Cmd) {
	// На этом экране главный - компонент списка.
	// Сначала отдаем сообщение ему. Он сам обработает и мышь, и клавиатуру.
	var cmd tea.Cmd
	m.dbList, cmd = m.dbList.Update(msg)

	// Теперь проверяем, не нажал ли пользователь клавишу, которая важна для нас
	if keyMsg, ok := msg.(tea.KeyMsg); ok {
		switch keyMsg.String() {
		case "q":
			Logger.Info("User requested to quit from database selection screen")
			return m, tea.Quit
		case " ":
			// Переключаем статус выбранного элемента
			if selectedItem, ok := m.dbList.SelectedItem().(item); ok {
				items := make([]list.Item, 0)
				for _, listItem := range m.dbList.Items() {
					if i, ok := listItem.(item); ok {
						if i.name == selectedItem.name {
							i.selected = !i.selected
						}
						items = append(items, i)
					}
				}
				m.dbList.SetItems(items)
			}
			return m, nil
		case "enter":
			// Собираем все выбранные базы данных
			selectedItems := make([]item, 0)
			for _, listItem := range m.dbList.Items() {
				if i, ok := listItem.(item); ok && i.selected {
					selectedItems = append(selectedItems, i)
				}
			}

			if len(selectedItems) > 0 {
				Logger.WithField("selected_count", len(selectedItems)).Info("Databases selected, transitioning to confirmation state")
				// Сохраняем выбранные БД
				m.selectedDBs = make([]string, len(selectedItems))
				for i, item := range selectedItems {
					m.selectedDBs[i] = item.name
				}
				m.state = stateConfirming
			} else {
				// Если ничего не выбрано, используем старую логику
				if selectedItem, ok := m.dbList.SelectedItem().(item); ok {
					m.selectedDBs = []string{selectedItem.name}
					Logger.WithField("selected_db", selectedItem.name).Info("Database selected, transitioning to confirmation state")
					m.state = stateConfirming
				}
			}
			return m, nil // Мы обработали событие, больше ничего не делаем
		}
	}
	return m, cmd // Возвращаем команду от компонента списка (например, для мигания курсора)
}

func (m model) updateConfirmingState(msg tea.Msg) (tea.Model, tea.Cmd) {
	if keyMsg, ok := msg.(tea.KeyMsg); ok {
		switch keyMsg.String() {
		case "y", "Y":
			Logger.Info("User confirmed migration start")
			m.state = stateMigrating
			m.setupMigrationSteps()
			return m, m.runCurrentStep() // Запускаем первый шаг
		case "n", "N":
			Logger.Info("User cancelled migration, returning to database selection")
			m.state = stateChoosingDB // Возвращаемся к выбору
			return m, nil
		}
	}

	return m, nil
}

func (m model) updateMigratingState(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if msg.String() == "q" {
			Logger.Info("User requested to quit during migration")
			return m, tea.Quit
		}
		if msg.String() == "l" {
			Logger.Info("User requested to view logs")
			m.state = stateViewingLogs
			return m, nil
		}
	case stepFinishedMsg:
		Logger.WithField("completed_step", m.currentStep).Info("Migration step completed")
		m.currentStep++
		percent := float64(m.currentStep) / float64(len(m.steps))
		progressCmd := m.progressBar.SetPercent(percent)

		if m.currentStep >= len(m.steps) {
			Logger.Info("All migration steps completed successfully")
			m.state = stateFinished
			return m, progressCmd
		}
		if m.steps[m.currentStep].name == "Переключение (Cutover)" {
			Logger.Info("Migration reached final cutover step, waiting for user confirmation")
			m.state = stateFinalCutoverPrompt
			return m, progressCmd
		}
		return m, tea.Batch(progressCmd, m.runCurrentStep())
	}

	return m, nil
}

func (m model) updateFinalCutoverPromptState(msg tea.Msg) (tea.Model, tea.Cmd) {
	if keyMsg, ok := msg.(tea.KeyMsg); ok {
		switch keyMsg.String() {
		case "y", "Y":
			Logger.Info("User confirmed final cutover, continuing migration")
			m.state = stateMigrating // Возвращаемся в состояние миграции для выполнения последнего шага
			return m, m.runCurrentStep()
		case "n", "N":
			Logger.Info("User cancelled final cutover")
			m.err = fmt.Errorf("миграция отменена пользователем перед финальным шагом")
			m.state = stateErrored
			return m, nil
		case "l", "L":
			Logger.Info("User requested to view logs from final cutover prompt")
			m.state = stateViewingLogs
			return m, nil
		}
	}

	return m, nil
}

func (m model) updateViewingLogsState(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if msg.String() == "q" || msg.String() == "esc" {
			// Возвращаемся к предыдущему состоянию
			if len(m.selectedDBs) > 0 && m.state != stateFinished && m.state != stateErrored {
				m.state = stateMigrating
			} else if m.state == stateFinished || m.state == stateErrored {
				m.state = stateFinished // или другое подходящее состояние
			} else {
				m.state = stateChoosingDB
			}
			return m, nil
		}
	case tea.WindowSizeMsg:
		// Обновляем размеры viewport при изменении размера окна
		m.logViewport.Width = msg.Width - 4
		m.logViewport.Height = msg.Height - 6

		// Читаем обновленное содержимое логов
		logContent, err := readLogContent()
		if err != nil {
			logContent = fmt.Sprintf("Ошибка чтения логов: %v", err)
		}

		m.logViewport.SetContent(logContent)
	}

	// Обновляем viewport
	m.logViewport, cmd = m.logViewport.Update(msg)
	return m, cmd
}

// Update - обработка всех событий
func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	// Сначала обрабатываем глобальные события, которые работают всегда
	switch msg := msg.(type) {
	case tea.KeyMsg:
		// Выход из приложения в любом состоянии
		if msg.String() == "ctrl+c" {
			Logger.Info("Received interrupt signal, exiting application")
			return m, tea.Quit
		}

		// Возможность просмотра логов из состояний ошибки и завершения
		if (m.state == stateErrored || m.state == stateFinished) && msg.String() == "l" {
			Logger.Info("User requested to view logs from error/finished state")
			m.state = stateViewingLogs
			return m, nil
		}
	case tea.WindowSizeMsg:
		m.dbList.SetWidth(msg.Width)

		// Обновляем размер logViewport при изменении размера окна
		if m.state == stateViewingLogs {
			m.logViewport.Width = msg.Width - 4
			m.logViewport.Height = msg.Height - 6

			// Читаем обновленное содержимое логов
			logContent, err := readLogContent()
			if err != nil {
				logContent = fmt.Sprintf("Ошибка чтения логов: %v", err)
			}

			m.logViewport.SetContent(logContent)
		}

		return m, nil
	case migrationErrorMsg:
		Logger.WithError(msg.err).WithField("current_step", m.currentStep).Error("Migration error occurred")
		m.err = msg.err
		m.state = stateErrored
		return m, nil
	}

	// Теперь обрабатываем логику в зависимости от текущего состояния (экрана)
	switch m.state {
	case stateChoosingDB:
		return m.updateChoosingDBState(msg)
	case stateConfirming:
		return m.updateConfirmingState(msg)
	case stateMigrating:
		return m.updateMigratingState(msg)
	case stateFinalCutoverPrompt:
		return m.updateFinalCutoverPromptState(msg)
	case stateViewingLogs:
		return m.updateViewingLogsState(msg)
	}

	// Обновляем спиннер и прогресс-бар независимо от остальной логики
	var spinCmd, progressCmd tea.Cmd
	var progressModel tea.Model

	m.spinner, spinCmd = m.spinner.Update(msg)

	progressModel, progressCmd = m.progressBar.Update(msg)
	m.progressBar = progressModel.(progress.Model)

	return m, tea.Batch(spinCmd, progressCmd)
}

// View - отрисовка интерфейса
func (m model) View() string {
	// Get terminal width for adaptive layout
	width := m.dbList.Width()

	switch m.state {
	case stateChoosingDB:
		var title string
		if m.isDryRun {
			// Добавляем заметный префикс к заголовку
			dryRunBanner := lipgloss.NewStyle().
				Background(lipgloss.Color("220")).
				Foreground(lipgloss.Color("0")).
				Padding(0, 1).
				Render("!!! DRY RUN MODE !!!")
			title = dryRunBanner + "\n\n"
		}
		title += m.dbList.View()
		// Apply border to the entire content
		borderedContent := borderStyle.Render(title)
		return docStyle.Render(borderedContent)

	case stateConfirming:
		var content string
		if len(m.selectedDBs) == 1 {
			// Если выбрана только одна БД, используем старый формат
			params := m.migrator.Params
			content = fmt.Sprintf(
				"Вы собираетесь мигрировать базу данных '%s'.\n\nПараметры:\n- StatefulSet: %s\n- NodePort: %d\n- Bare-Metal Host: %s\n\nНачать миграцию? (y/n)",
				params.DBName, params.K8sStatefulSetName, params.K8sNodePort, m.migrator.Config.BMHost,
			)
		} else {
			// Если выбрано несколько БД, показываем список
			var dbListStr string
			for i, dbName := range m.selectedDBs {
				if i > 0 {
					dbListStr += ", "
				}
				dbListStr += dbName
			}
			content = fmt.Sprintf(
				"Вы собираетесь мигрировать следующие базы данных: %s.\n\nВсего БД: %d\nBare-Metal Host: %s\n\nНачать миграцию? (y/n)",
				dbListStr, len(m.selectedDBs), m.migrator.Config.BMHost,
			)
		}
		// Apply border to the entire content
		borderedContent := borderStyle.Render(content)
		return docStyle.Render(borderedContent)

	case stateMigrating, stateFinalCutoverPrompt:
		// Создаем slice для хранения готовых строк
		var lines []string
		if len(m.selectedDBs) == 1 {
			lines = append(lines, "Идет миграция...\n")
		} else {
			lines = append(lines, fmt.Sprintf("Идет миграция %d баз данных...\n", len(m.selectedDBs)))
		}

		// Make progress bar adaptive to terminal width
		progressContent := m.progressBar.View()
		if width > 0 {
			progressContent = lipgloss.NewStyle().MaxWidth(width - 4).Render(progressContent)
		}
		lines = append(lines, progressContent)
		lines = append(lines, "")

		// Add a separator before the steps list
		lines = append(lines, separatorStyle.Render(strings.Repeat("─", width-4)))

		// Заполняем slice нестилизованными строками
		for i, step := range m.steps {
			var line string
			var statusIcon string

			switch m.stepStatus[i].status {
			case "pending":
				statusIcon = "[ ]" // ожидание (ASCII символы)
				line = fmt.Sprintf("%s %s", statusIcon, step.name)
				line = pendingStyle.Render(line)
			case "running":
				statusIcon = "[~]" // в процессе (ASCII символы)
				line = fmt.Sprintf("%s %s", statusIcon, step.name)
				line = runningStyle.Render(line)
			case "completed":
				statusIcon = "[v]" // завершено (ASCII символы)
				duration := m.stepStatus[i].duration.Truncate(time.Millisecond)
				line = fmt.Sprintf("%s %s (за %s)", statusIcon, step.name, duration)
				line = completedStyle.Render(line)
			case "failed":
				statusIcon = "[x]" // ошибка (ASCII символы)
				line = fmt.Sprintf("%s %s - ОШИБКА", statusIcon, step.name)
				line = failedStyle.Render(line)
			default:
				line = fmt.Sprintf(" %s", step.name)
				line = stepStyle.Render(line)
			}

			// Применяем стиль к каждой строке индивидуально
			lines = append(lines, line)

			// Если шаг завершен с ошибкой, добавляем сообщение об ошибке
			if m.stepStatus[i].status == "failed" {
				errorLine := fmt.Sprintf("    Сообщение об ошибке: %s", m.stepStatus[i].errorMsg)
				lines = append(lines, errorStyle.Render(errorLine))
			}
		}

		// Если мигрируем несколько БД, добавим информацию о состоянии каждой
		if len(m.selectedDBs) > 1 {
			lines = append(lines, separatorStyle.Render(strings.Repeat("─", width-4)))
			lines = append(lines, "Состояние баз данных:")
			for dbName, state := range m.multiMigrator.DBStates {
				var dbStatus string
				var statusIcon string
				var dbLine string

				switch state.Status {
				case "pending":
					statusIcon = "[ ]" // ожидание (ASCII символы)
					dbStatus = "ожидает"
					dbLine = pendingStyle.Render(fmt.Sprintf("  %s %s: %s", statusIcon, dbName, dbStatus))
				case "running":
					statusIcon = "[~]" // в процессе (ASCII символы)
					dbStatus = "в процессе"
					dbLine = runningStyle.Render(fmt.Sprintf("  %s %s: %s", statusIcon, dbName, dbStatus))
				case "completed":
					statusIcon = "[v]" // завершено (ASCII символы)
					dbStatus = "завершена"
					dbLine = completedStyle.Render(fmt.Sprintf("  %s %s: %s", statusIcon, dbName, dbStatus))
				case "failed":
					statusIcon = "[x]" // ошибка (ASCII символы)
					dbStatus = "ошибка"
					dbLine = failedStyle.Render(fmt.Sprintf("  %s %s: %s", statusIcon, dbName, dbStatus))
				default:
					dbLine = stepStyle.Render(fmt.Sprintf("  %s %s: %s", " ", dbName, state.Status))
				}

				// Добавляем информацию об ошибке, если она есть
				if state.Error != nil {
					dbLine += fmt.Sprintf(" - %v", state.Error)
				}

				lines = append(lines, dbLine)
			}
		}

		if m.state == stateFinalCutoverPrompt {
			lines = append(lines, separatorStyle.Render(strings.Repeat("─", width-4)))
			warningContent := warningStyle.Render("!!! ВНИМАНИЕ !!!")
			lines = append(lines, warningContent)
			lines = append(lines, "Репликация завершена. Следующий шаг вызовет ПРОСТОЙ приложения.")
			lines = append(lines, "Пожалуйста, остановите приложение, использующее эту базу.")
			lines = append(lines, "Начать финальное переключение? (y/n)")
		}

		helpContent := helpStyle.Render("Нажмите q или Ctrl+C для отмены.")
		logHelpContent := helpStyle.Render("Нажмите 'l' для просмотра логов.")
		lines = append(lines, fmt.Sprintf("\n%s", helpContent))
		lines = append(lines, logHelpContent)

		// Объединяем все строки и применяем общий стиль docStyle
		content := strings.Join(lines, "\n")
		if width > 0 {
			content = lipgloss.NewStyle().MaxWidth(width).Render(content)
		}
		return docStyle.Render(content)

	case stateFinished:
		var content string
		if len(m.selectedDBs) == 1 {
			content = fmt.Sprintf("✔ Миграция базы данных '%s' успешно завершена!", m.migrator.Params.DBName)
		} else {
			content = fmt.Sprintf("✔ Миграция %d баз данных успешно завершена!", len(m.selectedDBs))
		}
		borderedContent := borderStyle.Render(finalMsgStyle.Render(content))

		// Добавляем инструкцию о возможности просмотра логов
		helpContent := helpStyle.Render("Нажмите 'l' для просмотра логов.")
		borderedContent = lipgloss.JoinVertical(lipgloss.Top, borderedContent, helpContent)

		return docStyle.Render(borderedContent)

	case stateErrored:
		content := fmt.Sprintf("❌ Ошибка на шаге '%s':\n\n%v", m.steps[m.currentStep].name, m.err)
		borderedContent := borderStyle.Render(errorStyle.Render(content))

		// Добавляем инструкцию о возможности просмотра логов
		helpContent := helpStyle.Render("Нажмите 'l' для просмотра логов.")
		borderedContent = lipgloss.JoinVertical(lipgloss.Top, borderedContent, helpContent)

		return docStyle.Render(borderedContent)

	case stateViewingLogs:
		// Читаем содержимое лог-файла
		logContent, err := readLogContent()
		if err != nil {
			logContent = fmt.Sprintf("Ошибка чтения логов: %v", err)
		}

		// Устанавливаем содержимое в viewport
		m.logViewport.SetContent(logContent)

		// Создаем заголовок
		title := "Просмотр логов миграции (нажмите 'q' или 'Esc' для возврата)"

		// Комбинируем заголовок и viewport
		content := lipgloss.JoinVertical(
			lipgloss.Top,
			titleStyle.Render(title),
			borderStyle.Render(m.logViewport.View()),
		)

		return docStyle.Render(content)

	}
	return ""
}

// --- Хелперы и Команды ---

func (m *model) setupMigrationSteps() {
	// Проверяем, нужно ли использовать планировщик
	usePlanner := false // В реальном приложении это может быть опцией

	if usePlanner {
		// Создаем план миграции через планировщик
		options := MigrationPlanOptions{
			ParallelMigrations: false,
			MaxConcurrent:      1,
			RollbackOnFailure:  true,
			DryRun:             m.isDryRun,
		}

		plan, err := m.planner.CreatePlan(m.selectedDBs, options)
		if err != nil {
			Logger.WithError(err).Error("Failed to create migration plan")
			// В случае ошибки используем стандартный подход
			m.setupStandardMigrationSteps()
			return
		}

		// Валидируем план
		if err := m.planner.ValidatePlan(plan); err != nil {
			Logger.WithError(err).Error("Migration plan validation failed")
			// В случае ошибки используем стандартный подход
			m.setupStandardMigrationSteps()
			return
		}

		// Сохраняем план в модели для дальнейшего использования
		// Для простоты в этом примере мы продолжим использовать стандартный подход
		// Реализация полной интеграции с планировщиком требует более глубоких изменений
	}

	// Используем стандартный подход
	m.setupStandardMigrationSteps()
}

// setupStandardMigrationSteps устанавливает стандартные шаги миграции (существующая логика)
func (m *model) setupStandardMigrationSteps() {
	// Добавляем выбранные БД в multiMigrator
	for _, dbName := range m.selectedDBs {
		m.multiMigrator.AddDB(dbName)
	}

	m.steps = []migrationStep{
		{name: "Подготовка миграции нескольких БД", action: m.migrateMultipleDBs},
	}

	// Инициализируем статусы для всех шагов
	m.stepStatus = make([]stepStatus, len(m.steps))
	for i, step := range m.steps {
		m.stepStatus[i] = stepStatus{
			name:   step.name,
			status: "pending",
		}
	}
}

func (m *model) runCurrentStep() tea.Cmd {
	return func() tea.Msg {
		// Обновляем статус текущего шага
		m.stepStatus[m.currentStep].status = "running"
		m.stepStatus[m.currentStep].startTime = time.Now()

		Logger.WithField("step_name", m.steps[m.currentStep].name).Info("Starting migration step")
		time.Sleep(time.Millisecond * 500) // небольшая задержка для красоты
		err := m.steps[m.currentStep].action()
		m.stepStatus[m.currentStep].endTime = time.Now()
		m.stepStatus[m.currentStep].duration = m.stepStatus[m.currentStep].endTime.Sub(m.stepStatus[m.currentStep].startTime)

		if err != nil {
			Logger.WithError(err).WithField("step_name", m.steps[m.currentStep].name).Error("Migration step failed")
			m.stepStatus[m.currentStep].status = "failed"
			m.stepStatus[m.currentStep].errorMsg = err.Error()

			// Выполняем откат при ошибке
			Logger.Info("Starting rollback due to migration failure")
			rollbackErr := m.multiMigrator.RollbackAll()
			if rollbackErr != nil {
				Logger.WithError(rollbackErr).Error("Rollback failed")
				// Добавляем информацию об ошибке отката к основной ошибке
				err = fmt.Errorf("Migration failed: %v\nRollback error: %v", err, rollbackErr)
			} else {
				Logger.Info("Rollback completed successfully")
			}

			return migrationErrorMsg{err}
		}

		Logger.WithField("step_name", m.steps[m.currentStep].name).Info("Migration step completed successfully")
		m.stepStatus[m.currentStep].status = "completed"
		return stepFinishedMsg{}
	}
}

// migrateMultipleDBs выполняет миграцию нескольких БД
func (m *model) migrateMultipleDBs() error {
	// Проверяем, нужно ли использовать планировщик
	usePlanner := false // В реальном приложении это может быть опцией

	if usePlanner {
		// Создаем план миграции через планировщик
		options := MigrationPlanOptions{
			ParallelMigrations: false,
			MaxConcurrent:      1,
			RollbackOnFailure:  true,
			DryRun:             m.isDryRun,
		}

		plan, err := m.planner.CreatePlan(m.selectedDBs, options)
		if err != nil {
			Logger.WithError(err).Error("Failed to create migration plan")
			// В случае ошибки используем стандартный подход
			return m.multiMigrator.MigrateAll()
		}

		// Валидируем план
		if err := m.planner.ValidatePlan(plan); err != nil {
			Logger.WithError(err).Error("Migration plan validation failed")
			// В случае ошибки используем стандартный подход
			return m.multiMigrator.MigrateAll()
		}

		// Выполняем план миграции
		return m.planner.ExecutePlan(plan)
	}

	// Используем стандартный подход
	return m.multiMigrator.MigrateAll()
}

// readLogContent читает содержимое лог-файла
func readLogContent() (string, error) {
	file, err := os.Open("migration.log")
	if err != nil {
		return "", err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return "", err
	}

	return strings.Join(lines, "\n"), nil
}
