variable "subdomains" {
  type = "list"
  default = ["data1"]
}

variable "domain" {
  default = "dockpit.io"
}

resource "aws_s3_bucket" "data" {
  count = "${length(var.subdomains)}"
  bucket = "${element(var.subdomains, count.index)}.${var.domain}"
  versioning {
    enabled = true
  }
}

resource "aws_s3_bucket_policy" "b" {
  count = "${length(var.subdomains)}"
  bucket = "${element(aws_s3_bucket.data.*.bucket, count.index)}"
  policy = <<EOF
{
  "Version":"2008-10-17",
  "Statement":[{
    "Sid":"AllowPublicRead",
      "Effect":"Allow",
      "Principal": {"AWS": "*"},
      "Action":["s3:GetObject", "s3:PutObject"],
      "Resource":["arn:aws:s3:::${element(aws_s3_bucket.data.*.bucket, count.index)}/*"],
      "Condition": {
          "IpAddress": {
              "aws:SourceIp": [
                  "103.21.244.0/22",
                  "103.22.200.0/22",
                  "103.31.4.0/22",
                  "104.16.0.0/12",
                  "108.162.192.0/18",
                  "131.0.72.0/22",
                  "141.101.64.0/18",
                  "162.158.0.0/15",
                  "172.64.0.0/13",
                  "173.245.48.0/20",
                  "188.114.96.0/20",
                  "190.93.240.0/20",
                  "197.234.240.0/22",
                  "198.41.128.0/17",
                  "199.27.128.0/21"
              ]
          }
      }
    }
  ]
}
EOF
}

//DNS
resource "cloudflare_record" "data-bucket" {
  count = "${length(var.subdomains)}"
  domain = "${var.domain}"
  name = "${element(var.subdomains, count.index)}"
  value = "${element(aws_s3_bucket.data.*.id, count.index)}.s3-${element(aws_s3_bucket.data.*.region, count.index)}.amazonaws.com"
  proxied = true
  type = "CNAME"
  ttl = 1
}

output "s3_bucket" {
  value = "${aws_s3_bucket.data.bucket}"
}
