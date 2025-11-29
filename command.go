// command.go
package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/fatih/color"
	"github.com/sirupsen/logrus"
)

// Определим цвета для вывода, как в Python-скрипте
var (
	blue   = color.New(color.FgBlue, color.Bold).SprintFunc()
	green  = color.New(color.FgGreen).SprintFunc()
	yellow = color.New(color.FgYellow).SprintFunc()
	red    = color.New(color.FgRed).SprintFunc()
)

func printStep(title string)  { fmt.Printf("\n%s\n", blue("--- "+title+" ---")) }
func printSuccess(msg string) { fmt.Printf("%s\n", green("✔ "+msg)) }
func printWarning(msg string) { fmt.Printf("%s\n", yellow("⚠️ "+msg)) }
func printInfo(msg string)    { fmt.Printf("  %s\n", msg) }
func printError(msg string)   { fmt.Fprintln(os.Stderr, red("❌ "+msg)) }

// CommandOptions определяет дополнительные параметры для запуска команды
type CommandOptions struct {
	CaptureOutput bool
	Check         bool
	ExtraEnv      map[string]string
	Stdin         string
}

// RunCommand выполняет внешнюю команду
func RunCommand(name string, args []string, opts CommandOptions) (string, error) {
	Logger.WithFields(logrus.Fields{
		"command": name,
		"args":    strings.Join(args, " "),
	}).Debug("Executing command")

	cmd := exec.Command(name, args...)

	// Добавляем stdin, если он предоставлен
	if opts.Stdin != "" {
		cmd.Stdin = strings.NewReader(opts.Stdin)
	}

	// Устанавливаем переменные окружения
	if opts.ExtraEnv != nil {
		cmd.Env = os.Environ()
		for k, v := range opts.ExtraEnv {
			cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
		}
	}

	// Направляем вывод в наш буфер, чтобы TUI не ломался
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	err := cmd.Run()
	// Если есть ошибка и мы должны ее проверять...
	if err != nil && opts.Check {
		Logger.WithError(err).WithFields(logrus.Fields{
			"command": name,
			"args":    strings.Join(args, " "),
			"output":  out.String(),
		}).Error("Command failed")
		// ...то мы не печатаем ее здесь, а обогащаем и возвращаем наверх.
		// TUI сам решит, как ее красиво показать.
		errorMsg := fmt.Sprintf("команда '%s %s' завершилась с ошибкой: %v\n---\nВывод команды:\n%s",
			name,
			strings.Join(args, " "),
			err,
			out.String(),
		)
		return out.String(), fmt.Errorf(errorMsg)
	}

	Logger.WithFields(logrus.Fields{
		"command": name,
		"args":    strings.Join(args, " "),
	}).Debug("Command executed successfully")

	return strings.TrimSpace(out.String()), nil
}
