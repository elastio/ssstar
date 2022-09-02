function get_ssm_secret {
    local name="$1"

    aws ssm get-parameter --name "$name" --with-decryption --output json | jq ".Parameter.Value" -r
}
