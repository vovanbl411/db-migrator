// config.go
package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// AppConfig хранит основные настройки приложения
type AppConfig struct {
	ValuesFile   string `yaml:"valuesFile"`
	K8sNamespace string `yaml:"k8sNamespace"`
	BMHost       string `yaml:"bmHost"`
	BMSSHUser    string `yaml:"bmSSHUser"`
	BMPGUser     string `yaml:"bmPGUser"`
	NodePortBase int    `yaml:"nodePortBase"`
	DBNameSuffix string `yaml:"dbNameSuffix,omitempty"`
}

// StatefulSetTemplate описывает шаблон statefulset'а
type StatefulSetTemplate struct {
	Service struct {
		Port int `yaml:"port"`
	} `yaml:"service"`
	Image struct {
		Repository string `yaml:"repository"`
		Tag        string `yaml:"tag"`
	} `yaml:"image"`
	StartupProbe struct {
		TcpSocket bool `yaml:"tcpSocket"`
	} `yaml:"startupProbe"`
	ReadinessProbe struct {
		TcpSocket bool `yaml:"tcpSocket"`
	} `yaml:"readinessProbe"`
	LivenessProbe struct {
		TcpSocket bool `yaml:"livenessProbe"`
	} `yaml:"livenessProbe"`
	ConfigMap map[string]string `yaml:"configMap"`
	Volume    struct {
		Enabled          bool     `yaml:"enabled"`
		MountPath        string   `yaml:"mountPath"`
		StorageClassName string   `yaml:"storageClassName"`
		AccessModes      []string `yaml:"accessModes"`
		Size             string   `yaml:"size"`
	} `yaml:"volume"`
	NodeSelector map[string]string `yaml:"nodeSelector"`
	Sidecar      struct {
		Name            string `yaml:"name"`
		Image           string `yaml:"image"`
		ContainerPort   int    `yaml:"containerPort"`
		ServicePortName string `yaml:"servicePortName"`
		MetricsPath     string `yaml:"metricsPath"`
		Interval        string `yaml:"interval"`
		Scheme          string `yaml:"scheme"`
	} `yaml:"sidecar"`
}

// ValuesFile представляет структуру values-statefulsets.yaml
type ValuesFile struct {
	StatefulSetTemplates map[string]StatefulSetTemplate `yaml:"statefulset_templates"`
	StatefulSets         map[string]StatefulSet         `yaml:"statefulsets"`
}

// StatefulSet описывает конкретный statefulset в YAML
type StatefulSet struct {
	MergeTemplate string                   `yaml:"<<"`
	NodePort      int                      `yaml:"nodePort"`
	ReplicaCount  int                      `yaml:"replicaCount"`
	ConfigMap     StatefulSetConfig        `yaml:"configMap"`
	SidecarsEnv   map[string]string        `yaml:"sidecarsEnv"`
	Command       []string                 `yaml:"command"`
	Args          []string                 `yaml:"args"`
	NodeSelector  map[string]string        `yaml:"nodeSelector"`
	Tolerations   []map[string]interface{} `yaml:"tolerations"`
}

// StatefulSetConfig описывает конфигурацию statefulset'а
type StatefulSetConfig struct {
	DB       string                 `yaml:"POSTGRES_DB"`
	User     string                 `yaml:"POSTGRES_USER"`
	Password string                 `yaml:"POSTGRES_PASSWORD"`
	Settings map[string]interface{} `yaml:",inline"`
}

// LoadAppConfig загружает основную конфигурацию
// func LoadAppConfig() AppConfig {
// 	return AppConfig{
// 		ValuesFile:   "values-statefulsets.yaml",
// 		K8sNamespace: "dev",
// 		BMHost:       "172.17.1.22",
// 		BMSSHUser:    "v.korovayevich",
// 		BMPGUser:     "postgres",
// 		NodePortBase: 30000,
// 	}
// }

// LoadAppConfig теперь читает из файла
func LoadAppConfig(filePath string) (*AppConfig, error) {
	Logger.WithField("file_path", filePath).Debug("Loading application configuration")

	data, err := os.ReadFile(filePath)
	if err != nil {
		Logger.WithError(err).WithField("file_path", filePath).Error("Failed to read configuration file")
		return nil, fmt.Errorf("не удалось прочитать файл конфигурации %s: %w", filePath, err)
	}

	var config AppConfig
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		Logger.WithError(err).WithField("file_path", filePath).Error("Failed to unmarshal YAML configuration")
		return nil, fmt.Errorf("не удалось распарсить YAML из %s: %w", filePath, err)
	}

	Logger.WithField("file_path", filePath).Info("Successfully loaded application configuration")
	return &config, nil
}

// LoadValues читает и парсит YAML-файл со statefulset'ами
func LoadValues(filePath string) (*ValuesFile, error) {
	Logger.WithField("file_path", filePath).Debug("Loading values file")

	data, err := os.ReadFile(filePath)
	if err != nil {
		Logger.WithError(err).WithField("file_path", filePath).Error("Failed to read values file")
		return nil, err
	}

	// Сначала разбираем YAML в map для обработки шаблонов
	var rawValues map[string]interface{}
	err = yaml.Unmarshal(data, &rawValues)
	if err != nil {
		Logger.WithError(err).WithField("file_path", filePath).Error("Failed to unmarshal YAML values")
		return nil, err
	}

	// Преобразуем в ValuesFile
	var values ValuesFile

	// Обрабатываем statefulset_templates
	if templatesData, ok := rawValues["statefulset_templates"]; ok {
		if templatesMap, ok := templatesData.(map[string]interface{}); ok {
			values.StatefulSetTemplates = make(map[string]StatefulSetTemplate)
			for name, templateData := range templatesMap {
				// Преобразуем каждый шаблон в StatefulSetTemplate
				templateBytes, err := yaml.Marshal(templateData)
				if err != nil {
					Logger.WithError(err).WithField("template_name", name).Error("Failed to marshal template data")
					continue
				}

				var template StatefulSetTemplate
				err = yaml.Unmarshal(templateBytes, &template)
				if err != nil {
					Logger.WithError(err).WithField("template_name", name).Error("Failed to unmarshal template")
					continue
				}

				values.StatefulSetTemplates[name] = template
			}
		}
	}

	// Обрабатываем statefulsets
	if statefulsetsData, ok := rawValues["statefulsets"]; ok {
		if statefulsetsMap, ok := statefulsetsData.(map[string]interface{}); ok {
			values.StatefulSets = make(map[string]StatefulSet)
			for name, statefulsetData := range statefulsetsMap {
				// Преобразуем каждый statefulset в StatefulSet
				statefulsetBytes, err := yaml.Marshal(statefulsetData)
				if err != nil {
					Logger.WithError(err).WithField("statefulset_name", name).Error("Failed to marshal statefulset data")
					continue
				}

				var statefulset StatefulSet
				err = yaml.Unmarshal(statefulsetBytes, &statefulset)
				if err != nil {
					Logger.WithError(err).WithField("statefulset_name", name).Error("Failed to unmarshal statefulset")
					continue
				}

				// Обрабатываем специфичные для БД поля конфигурации
				if statefulsetDataMap, ok := statefulsetData.(map[string]interface{}); ok {
					if configMapData, ok := statefulsetDataMap["configMap"]; ok {
						if configMap, ok := configMapData.(map[string]interface{}); ok {
							statefulset.ConfigMap = StatefulSetConfig{
								DB:       getStringValue(configMap, "POSTGRES_DB"),
								User:     getStringValue(configMap, "POSTGRES_USER"),
								Password: getStringValue(configMap, "POSTGRES_PASSWORD"),
							}

							// Копируем остальные настройки
							for key, value := range configMap {
								if key != "POSTGRES_DB" && key != "POSTGRES_USER" && key != "POSTGRES_PASSWORD" {
									if statefulset.ConfigMap.Settings == nil {
										statefulset.ConfigMap.Settings = make(map[string]interface{})
									}
									statefulset.ConfigMap.Settings[key] = value
								}
							}
						}
					}
				}

				values.StatefulSets[name] = statefulset
			}
		}
	}

	Logger.WithField("file_path", filePath).Info("Successfully loaded values file")
	return &values, nil
}

// getStringValue вспомогательная функция для получения строкового значения из map
func getStringValue(data map[string]interface{}, key string) string {
	if value, ok := data[key]; ok {
		if strValue, ok := value.(string); ok {
			return strValue
		}
		// Если значение не строка, пробуем преобразовать
		return fmt.Sprintf("%v", value)
	}
	return ""
}
