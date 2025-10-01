resource "aws_s3_bucket" "stockpy_bucket" {
  bucket = "stockpy"
}

resource "aws_s3_object" "raw" {
  bucket = aws_s3_bucket.stockpy_bucket.bucket
  key    = "raw/"
}

resource "aws_s3_object" "refined" {
  bucket = aws_s3_bucket.stockpy_bucket.bucket
  key    = "refined/"
}

resource "aws_s3_object" "query_results" {
  bucket = aws_s3_bucket.stockpy_bucket.bucket
  key    = "query-results/"
}

resource "aws_s3_object" "scripts" {
  bucket = aws_s3_bucket.stockpy_bucket.bucket
  key    = "scripts/"
}
