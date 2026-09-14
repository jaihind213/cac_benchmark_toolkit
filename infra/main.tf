# infra/main.tf
# Provisions CAC serving VM (EC2) + Postgres (RDS) on AWS

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ── VPC / Networking ──────────────────────────────────────────────────────────

resource "aws_vpc" "cac" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  tags = { Name = "cac-vpc" }
}

resource "aws_internet_gateway" "cac" {
  vpc_id = aws_vpc.cac.id
  tags   = { Name = "cac-igw" }
}

resource "aws_subnet" "cac_public" {
  vpc_id                  = aws_vpc.cac.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "${var.aws_region}a"
  map_public_ip_on_launch = true
  tags = { Name = "cac-public-subnet" }
}

resource "aws_subnet" "cac_private_a" {
  vpc_id            = aws_vpc.cac.id
  cidr_block        = "10.0.2.0/24"
  availability_zone = "${var.aws_region}a"
  tags              = { Name = "cac-private-subnet-a" }
}

resource "aws_subnet" "cac_private_b" {
  vpc_id            = aws_vpc.cac.id
  cidr_block        = "10.0.3.0/24"
  availability_zone = "${var.aws_region}b"
  tags              = { Name = "cac-private-subnet-b" }
}

resource "aws_route_table" "cac_public" {
  vpc_id = aws_vpc.cac.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.cac.id
  }
  tags = { Name = "cac-public-rt" }
}

resource "aws_route_table_association" "cac_public" {
  subnet_id      = aws_subnet.cac_public.id
  route_table_id = aws_route_table.cac_public.id
}

# ── Security Groups ───────────────────────────────────────────────────────────

resource "aws_security_group" "cac_vm" {
  name   = "cac-vm-sg"
  vpc_id = aws_vpc.cac.id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.my_ip_cidr]   # SSH from your IP only
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "cac-vm-sg" }
}

resource "aws_security_group" "cac_postgres" {
  name   = "cac-postgres-sg"
  vpc_id = aws_vpc.cac.id

  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.cac_vm.id]   # VM only
  }
  tags = { Name = "cac-postgres-sg" }
}

# ── EC2 Serving VM ───────────────────────────────────────────────────────────

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"]   # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
}

resource "aws_instance" "cac_vm" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.cac_vm_instance_type
  subnet_id              = aws_subnet.cac_public.id
  vpc_security_group_ids = [aws_security_group.cac_vm.id]
  key_name               = var.ssh_key_name

  root_block_device {
    volume_size = var.cac_vm_disk_gb
    volume_type = "gp3"
    iops        = 3000
    throughput  = 125
  }

  user_data = <<-EOF
    #!/bin/bash
    apt-get update -y
    apt-get install -y software-properties-common
    add-apt-repository -y ppa:deadsnakes/ppa
    apt-get update -y
    apt-get install -y python3.11 python3.11-pip python3.11-venv git
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1
    python3.11 -m pip install --upgrade pip
    python3.11 -m pip install duckdb pyroaring pandas pyarrow pyyaml psycopg2-binary boto3 requests
    echo "CAC VM ready — Python $(python3 --version)"
  EOF

  tags = { Name = "cac-serving-vm" }
}

# ── RDS Postgres ─────────────────────────────────────────────────────────────

resource "aws_db_subnet_group" "cac" {
  name       = "cac-db-subnet-group"
  subnet_ids = [aws_subnet.cac_private_a.id, aws_subnet.cac_private_b.id]
  tags       = { Name = "cac-db-subnet-group" }
}

resource "aws_db_instance" "cac_postgres" {
  identifier        = "cac-postgres"
  engine            = "postgres"
  engine_version    = "15"
  instance_class    = var.postgres_instance_type
  allocated_storage = 20
  storage_type      = "gp3"

  db_name  = "cac"
  username = var.postgres_user
  password = var.postgres_password

  db_subnet_group_name   = aws_db_subnet_group.cac.name
  vpc_security_group_ids = [aws_security_group.cac_postgres.id]
  skip_final_snapshot    = true
  publicly_accessible    = false

  tags = { Name = "cac-postgres" }
}
