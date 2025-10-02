# role que será usada nos jobs
data "aws_iam_role" "glue_role" {
  name = var.role_name
}

resource "aws_glue_job" "extract_stocks_job" {
  name     = "extract_stocks_job"
  role_arn = data.aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "${var.script_location}extract_stocks_job.py"
  }

  default_arguments = var.extract_stocks_arguments

  max_retries       = 0
  worker_type       = "G.1X"
  number_of_workers = 2
}

resource "aws_glue_job" "extract_news_job" {
  name     = "extract_news_job"
  role_arn = data.aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "${var.script_location}extract_news_job.py"
  }

  default_arguments = var.extract_news_arguments

  max_retries       = 0
  worker_type       = "G.1X"
  number_of_workers = 2
}

resource "aws_glue_job" "transform_stocks_job" {
  name     = "transform_stocks_job"
  role_arn = data.aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "${var.script_location}transform_stocks_job.py"
  }

  default_arguments = var.default_arguments

  max_retries       = 0
  worker_type       = "G.1X"
  number_of_workers = 2
}

resource "aws_glue_job" "transform_news_job" {
  name     = "transform_news_job"
  role_arn = data.aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "${var.script_location}transform_news_job.py"
  }

  default_arguments = var.default_arguments

  max_retries       = 0
  worker_type       = "G.1X"
  number_of_workers = 2
}
