package pg

import (
	"fmt"
	"log"

	"github.com/jmoiron/sqlx"
)

type Pg struct {
	Host     string
	Port     int
	UserName string
	Password string
	DbName   string
}

var DbConnection *sqlx.DB

func (p *Pg) GetPgConnection() *sqlx.DB {
	if DbConnection == nil {
		appConfig := p
		pgInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			appConfig.Host,
			appConfig.Port,
			appConfig.UserName,
			appConfig.Password,
			appConfig.DbName)
		// pgInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", "127.0.0.1", 5432, "postgres", "postgres", "megatron")
		db, err := sqlx.Open("postgres", pgInfo)

		if err != nil {
			log.Fatalln(err)
		}
		DbConnection = db
		return DbConnection
	}
	return DbConnection
}
