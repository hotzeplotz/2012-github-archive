year = ARGV[0];
month = ARGV[1];
output_file = ARGV[2];

if (/\d{4}/.match(year) && /\d{1,2}/.match(month) && /\S+/.match(output_file))
  statement = sprintf("COPY (SELECT
   repository_url,
   repository_has_downloads,
   repository_created_at,
   repository_has_issues,
   repository_forks,
   repository_fork,
   repository_has_wiki,
   repository_homepage,
   repository_size,
   repository_private,
   repository_name,
   repository_owner,
   repository_open_issues,
   repository_watchers,
   repository_pushed_at,
   repository_language,
   repository_organization,
   repository_integrate_branch,
   repository_master_branch,
   actor_attributes_type,
   actor_attributes_login,
   actor_attributes_name,
   actor_attributes_company,
   actor_attributes_location,
   actor_attributes_email,
   created_at,
   public,
   actor,
   payload_head,
   payload_size,
   payload_ref,
   payload_master_branch,
   payload_ref_type,
   payload_description,
   payload_number,
   payload_action,
   payload_name,
   payload_url,
   payload_id,
   payload_desc,
   payload_commit,
   payload_after,
   payload_before,
   payload_commit_id,
   payload_commit_email,
   payload_commit_flag,
   url,
   type
  FROM events
  WHERE date_trunc('month', to_timestamp(created_at, 'YYYY-MM-DDTHH:MI:SS')) = '%04d-%02d-01 00:00:00'
  ORDER BY created_at)
  TO '%s' DELIMITER ',' CSV HEADER;", year, month, output_file)
  puts statement
else
  $stderr.puts "Syntax: #{$0} YYYY MM output_file"
end
