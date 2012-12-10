#
# $> ruby load_data.rb
#

require 'yajl'
require 'zlib'
require 'pg'
require 'csv'
require 'open-uri'
require 'date'

# define range of months to process year by year (dataset starts
# at some point in February 2011)
years = {
# 2011 => (2..12),
 2012 => (1..12)
}

$data_file_template = 'http://data.githubarchive.org/%4d-%02d-%02d-%d.json.gz'

# prepare the SQL table schema
@schema = open('https://raw.github.com/igrigorik/githubarchive.org/master/bigquery/schema.js')
@schema = Yajl::Parser.parse(@schema.read)
@keys = @schema.map {|r| r['name']}

# map GitHub JSON schema to flat CSV space based
# on provided Big Query column schema
def flatmap(h, e, prefix = '')
  e.each do |k,v|      
    if v.is_a?(Hash)
      flatmap(h, v, prefix+k+"_")
    else
      key = prefix+k
      next if !@keys.include? key
      
      case v
      when TrueClass then h[key] = 1
      when FalseClass then h[key] = 0
      else
        next if v.nil?
        h[key] = v unless v.is_a? Array
      end
    end
  end
  h
end

# check how many days are in a given month of a year
def days_in_month(year, month)
  (Date.new(year, 12, 31) << (12-month)).day
end

# Create table schema
create_table = "create table if not exists events ( \n"
@schema.each do |column|
  create_table += case column['type']
  when 'INTEGER', 'BOOLEAN'
    "#{column['name']} integer, \n"
  when 'STRING'
    "#{column['name']} text, \n"
  end
end
create_table = create_table.chomp(", \n") + ");"

# load the data
$db = PG::Connection.new({ :dbname => 'github'})
$db.exec(create_table)

def load_data(year, month, day, hour)
  dataset = sprintf("%4d-%02d-%02d-%dh", year, month, day, hour)
  start_time = Time.now
  begin
    $stdout.puts dataset + '--load-start'
    data_file = sprintf($data_file_template, year, month, day, hour)
    gz = open(data_file)
    js = Zlib::GzipReader.new(gz).read
    Yajl::Parser.parse(js) do |event|
      row = flatmap({}, event)
      keys, values = row.keys, row.values
      
      values_statement = []
      (1..keys.size).to_a.each do |x| 
        values_statement.push "$#{x}"
      end
    
      statement = "INSERT INTO events(#{keys.join(',')}) VALUES (#{(values_statement).join(',')})"
      $db.exec(statement, values)
    end
  rescue OpenURI::HTTPError => the_error
    the_status = the_error.io.status[0] # => 3xx, 4xx, or 5xx
    # the_error.message is the numeric code and text in a string
    $stderr.puts dataset + "--error--bad-status-code-#{the_error.message}"
  rescue SystemExit, Interrupt
    raise
  rescue Exception => msg
    $stderr.puts dataset + '--error'
  end
  end_time = Time.now
  $stdout.puts dataset + sprintf("--load-end@%d", (end_time - start_time)*1000)
  STDOUT.flush
end

years.each do |year, months|
  months.each do |month|
    (1..days_in_month(year, month)).each do |day|
      (0..23).each do |hour|
        load_data(year, month, day, hour)
      end
    end
  end
end
