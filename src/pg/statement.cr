class PG::Statement < ::DB::Statement
  def initialize(connection, command : String)
    super(connection, command)
  end

  protected def conn
    connection.as(Connection).connection
  end

  protected def perform_query(args : Enumerable) : ResultSet
    retry do
      begin
        params = args.map { |arg| PQ::Param.encode(arg) }
        conn = self.conn
        conn.send_parse_message(command)
        conn.send_bind_message params
        conn.send_describe_portal_message
        conn.send_execute_message
        conn.send_sync_message
        conn.expect_frame PQ::Frame::ParseComplete
        conn.expect_frame PQ::Frame::BindComplete
        frame = conn.read
        case frame
        when PQ::Frame::RowDescription
          fields = frame.fields
        when PQ::Frame::NoData
          fields = nil
        else
          raise "expected RowDescription or NoData, got #{frame}"
        end
        ResultSet.new(self, fields)
      rescue IO::Error
        raise DB::ConnectionLost.new(connection)
      end
    end
  end

  protected def perform_exec(args : Enumerable) : ::DB::ExecResult
    retry do
      begin
        result = perform_query(args)
        result.each { }
        ::DB::ExecResult.new(
          rows_affected: result.rows_affected,
          last_insert_id: 0_i64 # postgres doesn't support this
        )
      rescue IO::Error
        raise DB::ConnectionLost.new(connection)
      end
    end
  end

  protected def retry
    current_available = 0

    # Need to grab @idle, @retry_attempts, @retry_delay from DB URI
    # Deserialisation happens here: https://github.com/crystal-lang/crystal-db/blob/bf5ca75d1ace7e15b00ca03ad21728b8b00cf007/src/db/pool.cr
    idle = Set.new
    retry_attempts = 0
    retry_delay = 0.to_f64

    sync do
      current_available = idle.size
      # if the pool hasn't reach the max size, allow 1 attempt
      # to make a new connection if needed without sleeping
      current_available += 1 if can_increase_pool?
    end

    (current_available + retry_attempts).times do |i|
      begin
        sleep retry_delay if i >= current_available
        return yield
      rescue e : DB::PoolResourceLost(T)
        # if the connection is lost close it to release resources
        # and remove it from the known pool.
        sync { delete(e.resource) }
        e.resource.close
      rescue e : DB::PoolResourceRefused
        # a ConnectionRefused means a new connection
        # was intended to be created
        # nothing to due but to retry soon
      end
    end
    raise DB::PoolRetryAttemptsExceeded.new
  end
end
