// config_tester.go
// Функции для тестирования конфигурации

package main

import (
	"fmt"
	"log"
)

// TestConfig загружает и выводит информацию о конфигурации
func TestConfig() {
	// Загружаем конфигурацию
	appConfig, err := LoadAppConfig("config.yaml")
	if err != nil {
		log.Printf("Ошибка загрузки конфигурации: %v", err)
		return
	}

	values, err := LoadValues(appConfig.ValuesFile)
	if err != nil {
		log.Printf("Ошибка загрузки значений: %v", err)
		return
	}

	fmt.Printf("Конфигурация успешно загружена!\n")
	fmt.Printf("Количество StatefulSets: %d\n", len(values.StatefulSets))

	// Выводим информацию о базах данных
	for name, statefulset := range values.StatefulSets {
		if statefulset.ConfigMap.DB != "" {
			fmt.Printf("StatefulSet: %s, DB: %s, User: %s, NodePort: %d\n",
				name, statefulset.ConfigMap.DB, statefulset.ConfigMap.User, statefulset.NodePort)
		}
	}
}
