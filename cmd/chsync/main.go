// Команда chsync — перенос схемы ClickHouse (DDL) со старых шардов на новые хосты кластера.
//
// Вся логика — в internal/app; здесь только сигналы и код выхода.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"chsync/internal/app"
)

func main() {
	// Ctrl+C / SIGTERM отменяют контекст: текущий запрос к ClickHouse прерывается, apply останавливается.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := app.Run(ctx, os.Args[1:], app.OSStdio())
	stop()
	os.Exit(code)
}
