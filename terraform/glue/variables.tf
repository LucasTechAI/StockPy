variable "role_name" {
  default = "LabRole"
}

variable "script_location" {
  default = "s3://stockpy-bucket/scripts/"
}

variable "default_arguments" {
  type = map(string)
  default = {
    "--job-bookmark-option"       = "job-bookmark-disable"
    "--enable-glue-datacatalog"   = "true"
  }
}

variable "extract_stocks_arguments" {
  type = map(string)
  default = {
    "--job-bookmark-option"       = "job-bookmark-disable"
    "--enable-glue-datacatalog"   = "true"
    "--additional-python-modules" = "yfinance==0.2.65"
  }
}

variable "extract_news_arguments" {
  type = map(string)
  default = {
    "--job-bookmark-option"       = "job-bookmark-disable"
    "--enable-glue-datacatalog"   = "true"
    "--additional-python-modules" = "beautifulsoup4==4.13.5,requests==2.32.4"
  }
}

