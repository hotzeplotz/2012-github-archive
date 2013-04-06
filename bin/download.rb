# define range of months to process year by year (dataset starts
# at some point in February 2011
years = {
 2011 => (2..12),
 2012 => (1..12),
 2013 => (1..3)
}

# given year and month, download all data for the month;
# there will be some 404s as we ask for days 1 to 31 no matter which
# month we are processing
def download_data(year, month)
  month = sprintf('%02d', month)
  cmd = "wget http://data.githubarchive.org/#{year}-#{month}-{01..31}-{0..23}.json.gz"
end


years.each do |year, months|
  months.each do |month|
    cmd = download_data(year, month)
    print cmd + "\n"
  end
end
