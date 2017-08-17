require 'date'
# standalone protoype version


def go_direct(cmd)
  @fname = "/data/tmp/#{rand(999_999_999)}.csv"
  puts @fname
  %x{~/vsql -h dcvdbc10-int -B dcvdbc11-int,dcvdbc12-int  -U Reporter -w clarkkent -d sifi -F','  -AXtnqc  \"#{cmd}\"}
end

def dy2k
  y2k = Date.new(2000,1,1)
  (Date.today - y2k).to_i
end

def get_uid(line)
  uid = ''
  begin
    start = line.rindex('uid=') + 'uid='.size
    stop = line.index('"', start) -1
    uid = line[start .. stop]
  rescue
  end
  uid
end


# all user_ids that made it past the click-filtering 
def vertica_userids
  dd = dy2k() - 1
  cmd = "select distinct(sifi_uid) from cost_events where clicks > 0 and dd_id >= #{dd}"
  puts cmd
  s = go_direct(cmd)
  xs = s.split("\n")
  vus = {} 
  xs.each do |x|
    vus[x] = true
  end 
  vus
end


# all user-ids from the raw logs
def raw_userids
  users = {}
  cmd = "find /data/log/ctr -name  'ctr*.gz' -mtime -2 | xargs zcat"  
  IO.popen(cmd) do |io|
    while line = io.gets
      uid = get_uid(line)
      if uid and uid.size > 3
        users[uid] = true
      end
    end 
  end
  users
end

rus = raw_userids
puts rus.size
puts rus.keys[0..10].inspect

puts "--"

vus = vertica_userids
puts vus.size
puts vus.keys[0..10].inspect

puts "-------"
puts "working out which userids to blacklist"

to_delete = []
rus.keys.each do |k|
  to_delete << k unless vus[k]
end

puts "to delete: #{to_delete.size}"
puts to_delete[0..4].inspect

f = File.open("users_to_ban.txt", "w")
f.puts to_delete.join("\n")
f.close

puts "result in users_to_ban.txt"
