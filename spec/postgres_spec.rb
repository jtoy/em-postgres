$LOAD_PATH << File.join(File.dirname(__FILE__))
require "helper"
describe EventMachine::Postgres do
  
  it "should be true" do
    true.should be_true
  end
  it "should create a new connection" do
    EventMachine.run {
      lambda {
        conn = EventMachine::Postgres.new(:database => "test")
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
      #EM.add_periodic_timer(1){ puts }
      conn = EventMachine::Postgres.new(:database => "test")
      query = conn.execute("select 1;")
      
      query.callback{ |res|
        res.first["?column?"].should == "1"    
        EventMachine.stop
      }      
    }
  end

  it "should accept block as query callback" do
    EventMachine.run {
      conn = EventMachine::Postgres.new(:database => 'test')
      conn.execute("select 1;") { |res|
        res.first["?column?"].should == "1"
        EventMachine.stop
      }
    }
  end

  it "should accept paramaters" do
    EventMachine.run {
      conn = EventMachine::Postgres.new(:database => 'test')
      conn.execute("select $1::int AS first,$2::int AS second,$3::varchar AS third;",[1,nil,'']) { |res|        
        res.first["first"].should == "1"
        res.first["second"].should == nil
        res.first["third"].should == ""
        EventMachine.stop
      }
    }
  end

  it "allow custom error callbacks for each query" do
    EventMachine.run {
      conn = EventMachine::Postgres.new(:database => "test")
      query = conn.execute("select 1 from")
      query.errback { |res|
        #res.class.should == Mysql::Error
        1.should == 1
        EventMachine.stop
        1.should == 2 #we should never get here
      }
    }
  end


  it "queue up queries and execute them in order" do
    EventMachine.run {
      conn = EventMachine::Postgres.new(:database => 'test')

      results = []
      conn.execute("select 1 AS x;") {|res| puts res.inspect; results.push(res.first["x"].to_i)}
      conn.execute("select 2 AS x;") {|res| puts res.inspect;results.push(res.first["x"].to_i)}
      conn.execute("select 3 AS x;") {|res| puts res.inspect;results.push(res.first["x"].to_i)}
      EventMachine.add_timer(0.05) {
        results.should == [1,2,3]
        #conn.connection_pool.first.close
        
        EventMachine.stop
      }
    }
  end


  it "queue up large amount of queries and execute them in order" do
    EventMachine.run {

      conn = EventMachine::Postgres.new(:database => 'test')

      results = []
      (1..100).each do |i|
        conn.execute("select #{i} AS x;") {|res| results.push(res.first["x"].to_i)}

      end
      EventMachine.add_timer(1) {
        results.should == (1..100).to_a
        EventMachine.stop
      }
    }
  end


  it "should continue processing queries after hitting an error" do
    EventMachine.run {
      conn = EventMachine::Postgres.new(:database=> 'test')
      #errorback = Proc.new{
      #  true.should == true
        #EventMachine.stop
      #}
      q = conn.execute("select 1+ from table;") 
      q.errback{|r| puts "hi"; true.should == true } 
      conn.execute("select 1+1;"){ |res|
        res.first["?column?"].to_i.should == 2
        EventMachine.stop
      }
    }
  end

  it "should work with bind parameters" do
    EventMachine.run {
      conn = EventMachine::Postgres.new(:database=> 'test')
      conn.execute("select $1::int as bind1;",[4]){|r|
        r.first["bind1"].to_i.should == 4
      }
      conn.execute("select $1::text as bind1;",['four']){|r|
        r.first["bind1"].should == 'four'
        EventMachine.stop
      }
      
    }
  end
  
  

=begin
  it "should work with synchronous commands" do
    EventMachine.run {
      conn = EventMachine::Postgres #.new(:database => 'test')

      conn.list_dbs.class.should == Array
      conn.list_tables.class.should == Array
      conn.quote("select '1'").should == "select \\'1\\'"

      EventMachine.stop
    }
  end
=end
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