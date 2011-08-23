$LOAD_PATH << File.join(File.dirname(__FILE__))
require "helper"

describe EventMachine::Postgres do
  
  it "should be true" do
    true.should be_true
  end
  it "should create a new connection" do
    EventMachine.run {
      lambda {
        conn = EventMachine::Postgres.new(:database => "socmetrics")
        conn.connection.connected.should be_true
        conn.close
        conn.connection.connected.should be_false
        EventMachine.stop
      }.should_not raise_error
    }
  end

  it "should invoke errback on connection failure" do
    EventMachine.run {
      lambda {
        conn = EventMachine::Postgres.new({
            :host => 'localhost',
            :port => 20000,
            :socket => '',
            :errback => Proc.new {
              EventMachine.stop
            }
          })
      }.should_not raise_error
    }
  end

  it "should execute sql" do
    EventMachine.run {
      conn = EventMachine::Postgres.new(:database => "socmetrics")
      query = conn.query("select 1;")
      query.callback { |res|
        res.fetch_row.first.should == "1"
        EventMachine.stop
      }
    }
  end

  it "should accept block as query callback" do
    EventMachine.run {
      conn = EventMachine::Postgres.new(:host => 'localhost')
      conn.query("select 1") { |res|
        res.fetch_row.first.should == "1"
        EventMachine.stop
      }
    }
  end

  it "allow custom error callbacks for each query" do
    EventMachine.run {
      conn = EventMachine::Postgres.new(:host => 'localhost')
      query = conn.query("select 1 from")
      query.errback { |res|
        res.class.should == Mysql::Error
        EventMachine.stop
      }
    }
  end

  it "queue up queries and execute them in order" do
    EventMachine.run {
      conn = EventMachine::Postgres.new(:host => 'localhost')

      results = []
      conn.query("select 1") {|res| results.push res.fetch_row.first.to_i}
      conn.query("select 2") {|res| results.push res.fetch_row.first.to_i}
      conn.query("select 3") {|res| results.push res.fetch_row.first.to_i}

      EventMachine.add_timer(0.05) {
        results.should == [1,2,3]
        EventMachine.stop
      }
    }
  end

  it "should continue processing queries after hitting an error" do
    EventMachine.run {
      conn = EventMachine::Postgres.new(:host => 'localhost')

      conn.query("select 1+ from table")
      conn.query("select 1+1") { |res|
        res.fetch_row.first.to_i.should == 2
        EventMachine.stop
      }
    }
  end

  it "should work with synchronous commands" do
    EventMachine.run {
      conn = EventMachine::Postgres.new(:host => 'localhost', :database => 'test')

      conn.list_dbs.class.should == Array
      conn.list_tables.class.should == Array
      conn.quote("select '1'").should == "select \\'1\\'"

      EventMachine.stop
    }
  end

  #  it "should reconnect when disconnected" do
  #    # to test, run:
  #    # mysqladmin5 -u root kill `mysqladmin -u root processlist | grep "select sleep(5)" | cut -d'|' -f2`
  #
  #    EventMachine.run {
  #      conn = EventMachine::MySQL.new(:host => 'localhost')
  #
  #      query = conn.query("select sleep(5)")
  #      query.callback {|res|
  #        res.fetch_row.first.to_i.should == 0
  #        EventMachine.stop
  #      }
  #    }
  #  end

end