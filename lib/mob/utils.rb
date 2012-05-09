# encoding: utf-8
module Mob
  def script(options = {}, &block)
    file = options[:file] || options['file']
    file ||= eval('File.expand_path(__FILE__)', block.binding)
    script = Script.new(file, options, &block)
    name = (options[:name] || options['name'] || :worker).to_s
    script.name = name.to_s
    script.run(ARGV)
  end

  def callbacks
    @callbacks ||= {
      :before => {
        :fork => []
      },

      :after => {
        :fork => []
      }
    }
  end

  def before_fork(&block)
    list = callbacks[:before][:fork]
    if block
      list.push(block)
    else
      list.each{|block| block.call}
    end
  end

  def after_fork(&block)
    list = callbacks[:after][:fork]
    if block
      list.push(block)
    else
      list.reverse.each{|block| block.call}
    end
  end

  def cap?(&block)
    realpath = proc do |path|
      begin
        (path.is_a?(Pathname) ? path : Pathname.new(path.to_s)).realpath.to_s
      rescue Errno::ENOENT
        nil
      end
    end

    rails_root = realpath[Rails.root]

    shared_path = File.expand_path('../../shared', rails_root)
    cap_path = File.dirname(shared_path)
    shared_public_system_path = File.expand_path('../../shared/system')
    public_path = File.join(rails_root, 'public')

    public_system_path = File.join(public_path.to_s, 'system')
 
    is_cap_deploy =
      test(?e, shared_public_system_path) and
      test(?l, public_system_path) and
      realpath[shared_public_system_path] == realpath[public_system_path]

    return false unless is_cap_deploy

    args = 
      if block
        [cap_path].slice(block.arity > 0 ? (0 ... block.arity) : (0 .. -1))
      else
        []
      end
    block ? block.call(*args) : cap_path
  end

  def which_ruby
    c = ::RbConfig::CONFIG
    File::join(c['bindir'], c['ruby_install_name']) << c['EXEEXT']
  end

  def loggers
    @loggers ||= Hash.new do |hash, pid|
      tty_loggers = {
        true  => Mob::Logger.new(::Logger.new(STDERR), :prefix => "Mob"),
        false => Mob::Logger.new(Rails.logger, :prefix => "Mob")
      }
      hash.update(pid => tty_loggers)
    end
  end

  def logger
    loggers[Process.pid]
    loggers[Process.pid][STDOUT.tty?]
  end

  if defined?(MultiJson)
    def json_dump(object, options = {})
      MultiJson.dump(object, options) unless object.nil?
    end
    def json_load(string, options = {})
      MultiJson.load(string) unless string.nil?
    end
  else
    def json_dump(object, options = {})
      object.to_json unless object.nil?
    end
    def json_load(string, options = {})
      JSON.parse(string) unless string.nil?
    end
  end

  extend(Mob)
end
