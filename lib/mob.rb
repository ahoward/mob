# encoding: utf-8
module Mob
  require 'fileutils'
  require 'tmpdir'
  require 'ostruct'
  require 'rbconfig'
  require 'pathname'
  require 'socket'
  require 'pp'

  require 'mongoid'

  begin
    libdir = File.expand_path(__FILE__).sub(/\.rb\Z/, '')
    $LOAD_PATH.unshift(libdir)

    load 'utils.rb'
    load 'lock.rb'
    load 'logger.rb'
    load 'job.rb'
    load 'worker.rb'
    load 'cli.rb'
  ensure
    $LOAD_PATH.shift
  end

  unless defined?(Rails)
    Mongoid::Config.connect_to('mob') if Mongoid::Config.sessions[:default].nil?
  end

=begin
# FIXME
#
  Mob.before_fork do
    ::Mongoid.identity_map_enabled = false if defined?(::Mongoid.identity_map_enabled)
    ::Mongoid.master.connection.close if defined?(::Mongoid.master.connection.close)
  end

  Mob.after_fork do
    ::Mongoid.identity_map_enabled = false if defined?(::Mongoid.identity_map_enabled)
    ::Mongoid.master.connection.connect if defined?(::Mongoid.master.connection.connect)
  end
=end
end
