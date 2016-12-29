resource "random_id" "bucket" {
  byte_length = 8
}

resource "aws_s3_bucket" "sync" {
  bucket = "sync-${random_id.bucket.hex}"
}

resource "aws_s3_bucket_policy" "b" {
  bucket = "${aws_s3_bucket.sync.bucket}"
  policy = <<EOF
{
  "Version":"2008-10-17",
  "Statement":[{
    "Sid":"AllowPublicRead",
      "Effect":"Allow",
      "Principal": {"AWS": "*"},
      "Action":["s3:GetObject", "s3:PutObject"],
      "Resource":["arn:aws:s3:::${aws_s3_bucket.sync.bucket}/*"]
    }
  ]
}
EOF
}

output "s3_bucket" {
  value = "${aws_s3_bucket.sync.bucket}"
}
