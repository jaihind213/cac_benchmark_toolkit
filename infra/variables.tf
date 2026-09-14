# infra/variables.tf

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "ssh_key_name" {
  description = "Name of AWS key pair for SSH access to the VM"
  type        = string
}

variable "my_ip_cidr" {
  description = "Your IP address in CIDR format for SSH access e.g. 1.2.3.4/32"
  type        = string
}

variable "cac_vm_instance_type" {
  description = "EC2 instance type for CAC serving VM"
  type        = string
  default     = "r6i.xlarge"   # 4 vCPU, 32GB RAM ~$180/month
}

variable "cac_vm_disk_gb" {
  description = "Root disk size in GB for CAC serving VM"
  type        = number
  default     = 500
}

variable "postgres_instance_type" {
  description = "RDS instance type for Postgres (entity ID registry)"
  type        = string
  default     = "db.t3.medium"   # 2 vCPU, 4GB RAM — sufficient for ID mapping
}

variable "postgres_user" {
  description = "Postgres master username"
  type        = string
  default     = "cac"
}

variable "postgres_password" {
  description = "Postgres master password"
  type        = string
  sensitive   = true
}
