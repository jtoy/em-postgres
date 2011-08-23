
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

    #def initialize(array)
    def initialize(postgres,opts,conn) 
      puts s
      
      @conn = conn
      @postgres = postgres
      puts 'OOO'
      puts conn.inspect
      #@fd = #postgres.socket
      @fd = @conn.socket
      @opts = opts
      @current = nil
      @queue = []
      @processing = false
      @connected = true

      self.notify_readable = true
      EM.add_timer(0){ next_query }
    end

    def notify_readable
      
      puts 'IN HERE'
      if item = @current
        sql, cblk, eblk, retries = item
        result = @postgres.get_result

        # kick off next query in the background
        # as we process the current results
        @current = nil
        @processing = false
        next_query

        cblk.call(result)
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
        return close

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

      @connected = false

      EM.add_timer(0) do
        @processing = false
        @postgres = @conn.connect_socket(@opts)
        @fd = @postgres.socket

        @signature = EM.attach_fd(@postgres.socket, true)
        EM.set_notify_readable @signature, true
        EM.instance_variable_get('@conns')[@signature] = self
        @connected = true
        next_query
      end
    end

    def execute(sql, cblk = nil, eblk = nil, retries = 0)
      begin
        if not @processing or not @connected
          @processing = true
          #@postgres.query(sql)
          @conn.send_query(sql)
          
        else
          @queue << [sql, cblk, eblk, retries]
          return
        end

      rescue Exception => e
        puts "error in execute #{e}"
        if DisconnectErrors.include? e.message
          @queue << [sql, cblk, eblk, retries]
          return close
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
      detach
      @conn.finish
      @connected = false
    end

    private

      def next_query
        puts "not do"
        puts @queue
        if @connected and !@processing and pending = @queue.shift
          puts "do a query"
          sql, cblk, eblk = pending
          execute(sql, cblk, eblk)
        end
      end

  end
end