package main

import (
  "os"
  "io"
  "fmt"
  "log"
  "time"
  "bufio"
  "os/exec"
  "strings"
  "github.com/aws/aws-sdk-go/aws"
  "github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const pgCmd string = "pg_dump" // -t messages DBNAME > FILENAME.out

func main() {
  databases := os.Getenv("DATABASES")
  dbnames := strings.Split(databases, ",")

  for _, db := range dbnames {
    backup_path, err := createBackup(db)
    if err != nil {
      log.Fatal("Error: createBackup", err)
    }

    err = uploadFile(backup_path)
    if err != nil {
      log.Fatal("Error: uploadFile", err)
    }

    err = os.Remove(fmt.Sprintf("./%s", backup_path))
    if err != nil {
      log.Fatal("Error os.Remove: ", err)
    }
  }
}

func createBackup(db string) (path string, e error) {
  path = fmt.Sprintf("%s.%d.dump", db, time.Now().Unix())
  output, e := os.Create(fmt.Sprintf("./%s", path))
  if e != nil {
    log.Fatal("Error os.Create: ", e)
  }
  defer output.Close()

  cmd := exec.Command("pg_dump", "-t", "messages", db)
  stdout, e := cmd.StdoutPipe()
  if e != nil {
    log.Fatal("Error cmd.StdoutPipe: ", e)
  }

  writer := bufio.NewWriter(output)
  defer writer.Flush()

  if e = cmd.Start(); e != nil {
    log.Fatal("Error cmd.Start: ", e)
  }

  go io.Copy(writer, stdout)
  cmd.Wait()

  return
}

func uploadFile(path string) (e error) {
  aws.DefaultConfig.Region = "us-east-1"

  file, e := os.Open(path)
  if e != nil {
    return
  }

  uploader := s3manager.NewUploader(nil)
  _, e = uploader.Upload(&s3manager.UploadInput{
    Bucket: aws.String(os.Getenv("AWS_BUCKET")),
    Key: aws.String(file.Name()),
    Body: file,
  })

  return
}