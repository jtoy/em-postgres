
class Postgres
  def result
    @cur_result
  end
end

module EventMachine
  class PostgresConnection < EventMachine::Connection

    attr_reader :processing, :connected, :opts
    alias :settings :opts

    MAX_RETRIES_ON_DEADLOCKS = 10

    DisconnectErrors = [
      'query: not connected',
      'Postgres server has gone away',
      'Lost connection to Postgres server during query'
    ] unless defined? DisconnectErrors

    def initialize(postgres,opts,conn) 
    #def initialize(postgres,opts) 
      
      begin
      @conn = conn
      @postgres = postgres
      @fd = postgres.socket
      @opts = opts
      @current = nil
      @queue = []
      @processing = false
      @connected = true

      self.notify_readable = true
      EM.add_timer(0){ next_query }
    rescue => e
      puts e.inspect
    end
    end
    
    def self.connect(opts)
      if conn = connect_socket(opts)
        #debug [:connect, conn.socket, opts]
        EM.watch(conn.socket, EventMachine::PostgresConnection,conn,opts)
      else
        # invokes :errback callback in opts before firing again
        debug [:reconnect]
        EM.add_timer(5) { connect opts }
      end
    end
    
    # stolen from sequel
     def self.connect_socket(opts)
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

    def notify_readable
      if item = @current
        sql, cblk, eblk, retries = item
        #results = []
        #result = nil
        #@postgres.get_result{|r| result = r}
        #@postgres.get_result #TODO remove this, I can't process anymore code without this.
        result = nil        
        loop do
          # Fetch the next result. If there isn't one, the query is 
          # finished
          item = @postgres.get_result
          if item
            result = item
          else
            break
          end
          #puts "\n\nQuery result:\n%p\n" % [ result.values ]
        end
        
        unless @postgres.error_message == ""
          #TODO this is wrong
          eb = (eblk || @opts[:on_error])
          eb.call(result) if eb
          result.clear
          #reconnect
          @processing = false
          #@current = nil
          return next_query
        end
        # kick off next query in the background
        # as we process the current results
        @current = nil
        @processing = false
        cblk.call(result) if cblk
        result.clear
        next_query
      else
        return close
      end

    rescue Exception => e
      puts "error #{e}"
      if e.message =~ /Deadlock/ and retries < MAX_RETRIES_ON_DEADLOCKS
        @queue << [sql, cblk, eblk, retries + 1]
        @processing = false
        next_query

      elsif DisconnectErrors.include? e.message
        @queue << [sql, cblk, eblk, retries + 1]
        return #close

      elsif cb = (eblk || @opts[:on_error])
        cb.call(e)
        @processing = false
        next_query

      else
        raise e
      end
    end
    
    def unbind

      # wait for the next tick until the current fd is removed completely from the reactor
      #
      # in certain cases the new FD# (@mysql.socket) is the same as the old, since FDs are re-used
      # without next_tick in these cases, unbind will get fired on the newly attached signature as well
      #
      # do _NOT_ use EM.next_tick here. if a bunch of sockets disconnect at the same time, we want
      # reconnects to happen after all the unbinds have been processed

      #@connected = false
      EM.next_tick { reconnect }
    end
    
    def reconnect
      puts "DDDDD"
      @processing = false
      @postgres = @conn.connect_socket(@opts)
      @fd = @postgres.socket

      @signature = EM.attach_fd(@postgres.socket, true)
      EM.set_notify_readable(@signature, true)
      EM.instance_variable_get('@conns')[@signature] = self
      @connected = true
      next_query

    rescue Exception => e
      EM.add_timer(1) { reconnect }
    end


    def execute(sql, cblk = nil, eblk = nil, retries = 0)
      
      begin
        if not @processing or not @connected
        #if !@processing || !@connected
          @processing = true

          @postgres.send_query(sql)          
        else          
          @queue << [sql, cblk, eblk, retries]
          return
        end

      rescue Exception => e
        puts "error in execute #{e}"
        if DisconnectErrors.include? e.message
          @queue << [sql, cblk, eblk, retries]
          return #close
        else
          raise e
        end
      end
      @current = [sql, cblk, eblk, retries]
    end

    # act like the pg driver
    def method_missing(method, *args, &blk)
      @postgres.send(method, *args, &blk) if @postres.respond_to? method
    end

    def close
      return unless @connected
      detach
      @postgres.finish
      @connected = false
    end

    private

      def next_query
        if @connected and !@processing and pending = @queue.shift
          sql, cblk, eblk = pending
          execute(sql, cblk, eblk)
        end
      end

  end
end