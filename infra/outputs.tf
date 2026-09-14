# infra/outputs.tf

output "cac_vm_public_ip" {
  description = "Public IP of CAC serving VM — use for SSH"
  value       = aws_instance.cac_vm.public_ip
}

output "cac_vm_instance_type" {
  description = "EC2 instance type used for CAC serving VM"
  value       = aws_instance.cac_vm.instance_type
}

output "postgres_host" {
  description = "RDS Postgres endpoint"
  value       = aws_db_instance.cac_postgres.address
}

output "postgres_dsn" {
  description = "Postgres DSN for pipeline scripts"
  value       = "postgresql://${var.postgres_user}:${var.postgres_password}@${aws_db_instance.cac_postgres.address}:5432/cac"
  sensitive   = true
}
