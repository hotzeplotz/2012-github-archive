require 'open-uri'
require 'date'
require 'zlib'

module GitHubArchive
  def self.create_table(db, table_name, schema_file)
    #if(table_name !~ /^\s+$/)
    #  raise 'invalid table name supplied'
    #end
    
    # prepare the SQL table schema
    @schema = open(schema_file)
    @schema = Yajl::Parser.parse(@schema.read)
    @keys = @schema.map {|r| r['name']}
    
    # Create table schema
    create_table = "create table if not exists #{table_name} ( \n"
    @schema.each do |column|
      create_table += case column['type']
      when 'INTEGER', 'BOOLEAN'
        "#{column['name']} integer, \n"
      when 'TIMESTAMP'
        "#{column['name']} timestamp, \n"
      when 'STRING'
        "#{column['name']} text, \n"
      end
    end
    create_table = create_table.chomp(", \n") + ");"
    db.exec(create_table)
  end
  
  # map GitHub JSON schema to flat CSV space based
  # on provided Big Query column schema
  def self.flatmap(h, e, prefix = '')
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

  def self.load_data(db, table_name, source_file, year, month, day, hour)
    start_time = Time.now
    dataset = sprintf("%4d-%02d-%02d-%dh", year, month, day, hour)
    js = source_file
    
    $stdout.puts dataset + '--load-start'
    db.transaction() do
      Yajl::Parser.parse(js) do |event|
        begin
          row = flatmap({}, event)
          keys, values = row.keys, row.values
          
          values_statement = []
          (1..keys.size).to_a.each do |x| 
            values_statement.push "$#{x}"
          end
        
          statement = "INSERT INTO #{table_name}(#{keys.join(',')}) VALUES (#{(values_statement).join(',')})"
          puts statement
          db.exec(statement, values)
        rescue Exception => msg
          $stderr.puts dataset + '--error'
        end
      end
    end
    end_time = Time.now
    $stdout.puts dataset + sprintf("--load-end@%d", (end_time - start_time)*1000)
    STDOUT.flush
  end
  
  def self.download_chunk(year, month, day, hour)
    begin
      data_file_template = 'http://data.githubarchive.org/%4d-%02d-%02d-%d.json.gz'
      data_file = sprintf(data_file_template, year, month, day, hour)
      gz = open(data_file)
      js = Zlib::GzipReader.new(gz).read
    rescue OpenURI::HTTPError => the_error
      the_status = the_error.io.status[0] # => 3xx, 4xx, or 5xx
      # the_error.message is the numeric code and text in a string
      $stderr.puts dataset + "--error--bad-status-code-#{the_error.message}"
    end
    return js
  end

end
