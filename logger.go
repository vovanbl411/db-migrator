package main

import (
	"os"
	"time"

	"github.com/sirupsen/logrus"
)

var Logger *logrus.Logger

func InitLogger(outputToStdout bool) {
	Logger = logrus.New()

	// Установим формат вывода JSON для лучшего аудита
	Logger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339,
	})

	// Установим уровень логирования по умолчанию
	Logger.SetLevel(logrus.InfoLevel)

	// Запишем логи в файл
	file, err := os.OpenFile("migration.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err == nil {
		if outputToStdout {
			// Если нужно выводить в stdout, создаем MultiWriter
			Logger.SetOutput(os.Stdout)
		} else {
			// В TUI режиме - только в файл
			Logger.SetOutput(file)
		}
	} else {
		Logger.SetOutput(os.Stdout)
		Logger.Warn("Не удалось создать файл лога, используем stdout")
	}
}

func init() {
	// Инициализация будет выполнена в main() в зависимости от режима работы
	// Здесь оставляем минимальную инициализацию для избежания nil-указателя
	Logger = logrus.New()
}

// PerformanceLogger - структура для отслеживания метрик производительности
type PerformanceLogger struct {
	startTime time.Time
	metrics   map[string]time.Duration
}

// NewPerformanceLogger - создает новый экземпляр PerformanceLogger
func NewPerformanceLogger() *PerformanceLogger {
	return &PerformanceLogger{
		startTime: time.Now(),
		metrics:   make(map[string]time.Duration),
	}
}

// StartTimer - начинает отсчет времени для операции
func (pl *PerformanceLogger) StartTimer(operation string) {
	pl.metrics[operation+"-start"] = time.Since(pl.startTime)
}

// EndTimer - заканчивает отсчет времени для операции и логирует метрики
func (pl *PerformanceLogger) EndTimer(operation string) time.Duration {
	startTime, ok := pl.metrics[operation+"-start"]
	if !ok {
		Logger.WithField("operation", operation).Warn("EndTimer called without StartTimer")
		return 0
	}

	duration := time.Since(pl.startTime) - startTime
	pl.metrics[operation] = duration

	Logger.WithFields(logrus.Fields{
		"operation": operation,
		"duration":  duration.String(),
	}).Info("Operation completed")

	return duration
}

// LogMetric - логирует произвольную метрику
func (pl *PerformanceLogger) LogMetric(name string, value interface{}) {
	Logger.WithFields(logrus.Fields{
		"metric_name":  name,
		"metric_value": value,
	}).Info("Performance metric recorded")
}

// GetMetrics - возвращает все собранные метрики
func (pl *PerformanceLogger) GetMetrics() map[string]time.Duration {
	return pl.metrics
}
