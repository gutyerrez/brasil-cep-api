package exporter

import (
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/brasilcep/api/database"
	"github.com/brasilcep/api/logger"
	"github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
)

type Exporter struct {
	logger *logger.Logger
}

func NewExporter(logger *logger.Logger) *Exporter {
	return &Exporter{logger: logger}
}

func (exporter *Exporter) ExportToCSV() {
	db := database.GetDB()

	wd, err := os.Getwd()

	if err != nil {
		exporter.logger.Error("Erro ao obter diretório atual: %v", zap.Error(err))

		return
	}

	tmpDir := filepath.Join(wd, "tmp")

	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		exporter.logger.Error("Erro ao criar diretório tmp: %v", zap.Error(err))

		return
	}

	var fileName = filepath.Join(tmpDir, "ceps.csv")

	f, err := os.Create(fileName)

	if err != nil {
		exporter.logger.Error("Erro ao criar arquivo CSV: %v", zap.Error(err))
		return
	}

	defer f.Close()

	writer := csv.NewWriter(f)

	defer writer.Flush()

	keysCh := make(chan string, 1000)
	recordsCh := make(chan []string, 1000)

	var writeErr error
	var writerWG sync.WaitGroup

	writerWG.Add(1)

	go func() {
		defer writerWG.Done()

		for rec := range recordsCh {
			if err := writer.Write(rec); err != nil {
				exporter.logger.Error("Erro ao escrever linha CSV: %v", zap.Error(err))

				writeErr = err

				return
			}
		}
	}()

	numWorkers := runtime.NumCPU()

	var wg sync.WaitGroup

	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()

			for key := range keysCh {
				_ = db.View(func(txn *badger.Txn) error {
					item, err := txn.Get([]byte(key))

					if err != nil {
						exporter.logger.Error("Erro ao obter valor do BadgerDB: %v", zap.Error(err))
						return nil
					}

					return item.Value(func(val []byte) error {
						var data map[string]interface{}

						if err := json.Unmarshal(val, &data); err != nil {
							return nil
						}

						rec := []string{
							strings.TrimPrefix(key, "cep:"),
							exporter.getString(data, "bairro"),
							exporter.getString(data, "cep"),
							exporter.getString(data, "cidade"),
							exporter.getString(data, "codigo_ibge"),
							exporter.getString(data, "logradouro"),
							exporter.getString(data, "nome_origem"),
							exporter.getString(data, "tipo_origem"),
							exporter.getString(data, "uf"),
						}

						recordsCh <- rec

						return nil
					})
				})
			}
		}()
	}

	produced := 0

	err = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions

		opts.PrefetchSize = 10

		it := txn.NewIterator(opts)

		defer it.Close()

		searchPrefix := []byte("cep:")

		for it.Seek(searchPrefix); it.ValidForPrefix(searchPrefix); it.Next() {
			item := it.Item()

			k := append([]byte{}, item.Key()...)

			keysCh <- string(k)

			produced++
		}

		return nil
	})

	if err != nil {
		exporter.logger.Error("Erro ao iterar chaves: %v", zap.Error(err))

		close(keysCh)
		close(recordsCh)

		return
	}

	close(keysCh)

	wg.Wait()

	close(recordsCh)

	writerWG.Wait()

	if writeErr != nil {
		exporter.logger.Error("Erro na escrita do CSV: %v", zap.Error(writeErr))
		return
	}

	if err := writer.Error(); err != nil {
		exporter.logger.Error("Erro ao finalizar escrita do CSV: %v", zap.Error(err))
		return
	}

	exporter.logger.Info("Total CEPs exportados: %d", zap.Int("total", produced))
}

func (e *Exporter) getString(cep map[string]interface{}, key string) string {
	if cep == nil {
		return ""
	}

	v, ok := cep[key]

	if !ok || v == nil {
		return ""
	}

	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	default:
		return ""
	}
}
