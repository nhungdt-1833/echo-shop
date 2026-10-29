package mail

import (
	"context"
	"log"
)

type Mailer interface {
	Send(ctx context.Context, to []string, subject, body string) error
}

type LogMailer struct{}

func (LogMailer) Send(_ context.Context, to []string, subject, body string) error {
	log.Printf("[mail] 📧 to=%v subject=%q\n%s", to, subject, body)
	return nil
}
