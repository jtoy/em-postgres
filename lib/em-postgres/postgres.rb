require "eventmachine"
require "pg"
require "fcntl"

=begin
module EventMachine
  class Postgres

  def self.settings
    @settings ||= { :connections => 1, :logging => false,:database=>"test" }
  end

  def self.execute query, cblk = nil, eblk = nil, &blk
    @n ||= 0
    connection = connection_pool[@n]
    @n = 0 if (@n+=1) >= connection_pool.size

    #connection.execute(query, type, cblk, eblk, &blk)
    
    df = EventMachine::DefaultDeferrable.new
    cb = blk || Proc.new { |r| df.succeed(r) }
    eb = Proc.new { |r| df.fail(r) }
    connection.execute(query,cb, eb)
    df
  end
  #class << self
  #  alias query execute 
  #end
  def self.connection_pool
    @connection_pool ||= (1..settings[:connections]).map{ EventMachine::PostgresConnection.connect(settings) }
    
  end
end
end
=end


module EventMachine
  class Postgres

    #self::Postgres = ::Postgres unless defined? self::Postgres

    attr_reader :connection

    def initialize(opts)
      unless EM.respond_to?(:watch) and PGconn.method_defined?(:socket)
        
        raise RuntimeError, 'pg and EM.watch are required for EventedPostgres'
      end

      @settings = { :debug => false }.merge!(opts)
      @connection = connect(@settings)
    end

    def close
      @connection.close
    end

    def query(sql,params=[], &blk)
      df = EventMachine::DefaultDeferrable.new
      cb = blk || Proc.new { |r| df.succeed(r) }
      eb = Proc.new { |r| df.fail(r) }
      
      @connection.execute(sql,params,cb,eb)
      df
    end
    alias :real_query :query
    alias :execute :query
    # behave as a normal postgres connection
    def method_missing(method, *args, &blk)
      @connection.send(method, *args)
    end

    def connect(opts)
      if conn = connect_socket(opts)
        #debug [:connect, conn.socket, opts]
        #EM.watch(conn.socket, EventMachine::PostgresConnection, conn, opts, self)

        EM.watch(conn.socket, EventMachine::PostgresConnection,conn,opts,self)
      else
        # invokes :errback callback in opts before firing again
        debug [:reconnect]
        EM.add_timer(5) { connect opts }
      end
    end

    # stolen from sequel
    def connect_socket(opts)
    begin
      conn = PGconn.connect(
        opts[:host],
        (opts[:port]), #TODO deal with host and port
        nil,nil,
        opts[:database],
        opts[:user],
        opts[:password]
      )
      # set encoding _before_ connecting
      if encoding = opts[:encoding] || opts[:charset]
        if conn.respond_to?(:set_client_encoding)
          conn.set_client_encoding(encoding)
        else
          conn.async_exec("set client_encoding to '#{encoding}'")
        end
      end
      
      #conn.options(Mysql::OPT_LOCAL_INFILE, 'client')
 
      # increase timeout so mysql server doesn't disconnect us
      # this is especially bad if we're disconnected while EM.attach is
      # still in progress, because by the time it gets to EM, the FD is
      # no longer valid, and it throws a c++ 'bad file descriptor' error
      # (do not use a timeout of -1 for unlimited, it does not work on mysqld > 5.0.60)
      #conn.query("set @@wait_timeout = #{opts[:timeout] || 2592000}")

      # we handle reconnecting (and reattaching the new fd to EM)
      #conn.reconnect = false

      # By default, MySQL 'where id is null' selects the last inserted id
      # Turn this off. http://dev.rubyonrails.org/ticket/6778
      #conn.query("set SQL_AUTO_IS_NULL=0")

      # get results for queries
      #conn.query_with_result = true

      conn
    rescue Exception => e
      puts "#{e} exception"
      if cb = opts[:errback]
        cb.call(e)
        nil
      else
        raise e
      end
    end
    end

    def debug(data)
      p data if @settings[:debug]
    end
  end
end
