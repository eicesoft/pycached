<?php
error_reporting(0);

class PyCache {
    const CMD_SET = 0;
    const CMD_GET = 1;
    const CMD_DELETE = 2;
    const CMD_EXISTS = 3;
    const CMD_EXPIRE = 4;
    const CMD_PERSIST = 5;
    const CMD_TTL = 6;
    CONST CMD_RENAME = 7;

    const CMD_LPUSH = 100;
    const CMD_LPOP = 101;
    const CMD_LRANGE = 102;
    const CMD_LLEN = 103;
    const CMD_LINDEX = 104;
    const CMD_LINSERT = 105;
    
    const CMD_HMSET = 200;
    const CMD_HSET = 201;
    const CMD_HGET = 202;
    const CMD_HGETALL = 203;
    const CMD_HEXISTS = 204;
    const CMD_HLEN = 205;
    const CMD_HDEL = 206;
    const CMD_HKEYS = 207;
    const CMD_HVALS = 208;

    const CMD_STATUS = 9000;
    const CMD_SAVE = 9999;
    
    private $handle;
    private $is_connect;
    private $is_slave;

    public function __construct($host, $port, $is_slave=False)
    {
        $old = error_reporting(0);
        $this->handle = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
        $this->is_connect = socket_connect($this->handle, $host, $port);
        error_reporting($old);

        if(!$this->is_connect) {
            throw new Exception(sprintf("Connection Parker server[%s] fails.", $host, $port), -10010);
        } else {
            $this->is_slave = $is_slave;
            //超时设置
            socket_set_option($this->handle, SOL_SOCKET, SO_SNDTIMEO, array("sec" => 5, "usec" => 0));
            socket_set_option($this->handle, SOL_SOCKET, SO_RCVTIMEO, array("sec" => 5, "usec" => 0));
        }
    }

    public function set($key, $val, $expire=0, $flag=0)
    {
        $val = gzcompress($val);

        $body = $this->build_string($val);
        $body .= $this->build_int($expire);
        $body .= $this->build_int($flag);

        $package = $this->build_base_package(PyCache::CMD_SET, $key, $body);
        
        $this->_write_client($package);
        return $this->parse_result_int();
    }
    
    public function get($key)
    {
        $package = $this->build_base_package(PyCache::CMD_GET, $key);

        $this->_write_client($package);
        $data = $this->parse_result_string();
        // echo "[", $data, "]";
        if ($data !== null) {
            return gzuncompress($data);
        } else {
            return null;
        }
    }

    public function delete($key)
    {
        $package = $this->build_base_package(PyCache::CMD_DELETE, $key);
        
        $this->_write_client($package);
        return $this->parse_result_int();
    }
    
    public function exists($key)
    {
        $package = $this->build_base_package(PyCache::CMD_EXISTS, $key);
        
        $this->_write_client($package);
        return (bool)$this->parse_result_int();
    }
    
    public function presist($key)
    {
        $package = $this->build_base_package(PyCache::CMD_PERSIST, $key);
        
        $this->_write_client($package);
        return $this->parse_result_int();
    }
    
    public function ttl($key)
    {
        $package = $this->build_base_package(PyCache::CMD_TTL, $key);
        
        $this->_write_client($package);
        return (int)$this->parse_result_string();
    }
    
    public function expire($key, $expire)
    {
        $body = $this->build_int($expire);
        $package = $this->build_base_package(PyCache::CMD_EXPIRE, $key, $body);
        
        $this->_write_client($package);
        return $this->parse_result_string();
    }
    
    public function rename($key, $newkey)
    {
        $body = $this->build_string($newkey);
        $package = $this->build_base_package(PyCache::CMD_RENAME, $key, $body);
        
        $this->_write_client($package);
        return $this->parse_result_int();
    }

    public function lpush($key, $val)
    {
        $body = $this->build_string($val);
        $package = $this->build_base_package(PyCache::CMD_LPUSH, $key, $body);
        
        $this->_write_client($package);
        
        return $this->parse_result_int();
    }

    public function lrange($key, $start=0, $end=-1)
    {
        $body = $this->build_int($start);
        $body .= $this->build_int($end);
        
        $package = $this->build_base_package(PyCache::CMD_LRANGE, $key, $body);
        
        $this->_write_client($package);
        $data = $this->parse_result_string();
        
        if ($data !== null) {
            return json_decode($data, true);
        } else {
            return null;
        }
    }

    public function lpop($key)
    {
        $package = $this->build_base_package(PyCache::CMD_LPOP, $key);

        $this->_write_client($package);
        $data = $this->parse_result_string();
        return $data;
    }

    public function llen($key)
    {
        $package = $this->build_base_package(PyCache::CMD_LLEN, $key);
        
        $this->_write_client($package);
        $data = $this->parse_result_string();
        return intval($data);
    }

    public function lindex($key, $index)
    {
        $body = $this->build_int($index);
        $package = $this->build_base_package(PyCache::CMD_LINDEX, $key, $body);
        
        $this->_write_client($package);
        $data = $this->parse_result_string();
        return $data;
    }

    public function linsert($key, $index, $val)
    {
        $body = $this->build_int($index);
        $body .= $this->build_string($val);
        $package = $this->build_base_package(PyCache::CMD_LINSERT, $key, $body);
        
        $this->_write_client($package);
        return $this->parse_result_int();
    }
    
    public function hmset($key, $values)
    {
        $body = $this->build_string(json_encode($values));
        $package = $this->build_base_package(PyCache::CMD_HMSET, $key, $body);
        
        $this->_write_client($package);
        return $this->parse_result_int();
    }
    
    public function hset($key, $field, $val)
    {
        $body = $this->build_string($field);
        $body .= $this->build_string($val);
        $package = $this->build_base_package(PyCache::CMD_HSET, $key, $body);
        
        $this->_write_client($package);
        return $this->parse_result_int();
    }
    
    public function hget($key, $fields)
    {
        $body = $this->build_string(json_encode($fields));
        $package = $this->build_base_package(PyCache::CMD_HGET, $key, $body);
        
        $this->_write_client($package);
        $data = $this->parse_result_string();
        
        return json_decode($data, true);
    }
    
    public function hgetall($key)
    {
         $package = $this->build_base_package(PyCache::CMD_HGETALL, $key);
         
         $this->_write_client($package);
         
         $data = $this->parse_result_string();
         
         return json_decode($data, true);
    }
    
    public function hexists($key, $field)
    {
        $body = $this->build_string($field);
        $package = $this->build_base_package(PyCache::CMD_HEXISTS, $key, $body);
        
        $this->_write_client($package);
        
        $data = $this->parse_result_string();
        return (bool)$data;
    }
    
    public function hlen($key)
    {
         $package = $this->build_base_package(PyCache::CMD_HLEN, $key);
         
         $this->_write_client($package);
         
         $data = $this->parse_result_string();
         
         return (int)$data;
    }
    
    public function hdel($key, $fields)
    {
        $body = $this->build_string(json_encode($fields));
        $package = $this->build_base_package(PyCache::CMD_HDEL, $key, $body);
         
        $this->_write_client($package);
         
        $data = $this->parse_result_int();
         
        return $data;
    }
    
    public function hkeys($key)
    {
         $package = $this->build_base_package(PyCache::CMD_HKEYS, $key);
         
         $this->_write_client($package);
         
         $data = $this->parse_result_string();
         
         return json_decode($data, true);
    }
    
    public function hvals($key)
    {
         $package = $this->build_base_package(PyCache::CMD_HVALS, $key);
         
         $this->_write_client($package);
         
         $data = $this->parse_result_string();
         
         return json_decode($data, true);
    }
    
    public function status()
    {
        $package = $this->build_base_package(PyCache::CMD_STATUS, '');
        
        $this->_write_client($package);
        
        return json_decode($this->parse_result_string(), true);
    }
    
    public function save()
    {
        $package = $this->build_base_package(PyCache::CMD_SAVE, '');
        
        $this->_write_client($package);
        
        return $this->parse_result_int();
    }
    
    private function _write_client($package)
    {
        socket_write($this->handle, $package, strlen($package));
    }
    
    /**
     * 解析int型结果
     *
     * @return string
     * @author kelezyb
     */
    private function parse_result_int()
    {
        $data = socket_read($this->handle, 4);
        
        $l = $this->parse_int($data);
        // print $l;
        $data = socket_read($this->handle, $l);
        return $this->parse_int($data);
    }
    
    /**
     * 解析字符串型结果
     *
     * @return string
     * @author kelezyb
     */
    private function parse_result_string()
    {
        $data = socket_read($this->handle, 4);
        // print $data;
        $l = $this->parse_int($data);
        // print $l;
        $data = socket_read($this->handle, $l);
        $code = $this->parse_int(substr($data, 0, 4));
        // echo $code, $data;
        if ($code == 1) {
            return substr($data, 4);
        } else {
            return null;
        }
    }

    /**
     * 构造基本数据包
     *
     * @param string $cmd
     * @param string $key
     * @param string $extend
     * @return string
     */
    private function build_base_package($cmd, $key, $extend='')
    {
        $body = '';
        $body .= $this->build_int($cmd);
        $body .= $this->build_string($key);
        $body .= $extend;
        if ($this->is_slave) {
            $body .= $this->build_int(0);
        }
        $package = $this->build_string($body);
        return $package;
    }
    
    private function build_int($l)
    {
        return pack('N', $l);
    }

    private function build_string($val)
    {
        $body = $this->build_int(strlen($val));
        $body .= $val;
        return $body;
    }

    private function parse_int($data)
    {
        list(, $ret) = unpack('N', $data);
        return $ret;
    }
}

//$p = new PyCache('172.17.0.64', 11311);

try {
    // $p = new PyCache('127.0.0.1', 11311);
    $p = new PyCache('127.0.0.1', 11312, True);
    $start = microtime(true);

    // $p->delete('testhash');
    // $p->hmset('testhash', array('test222'=>'test', 'abc' => 'sdfds2'));
    // $p->hset('testhash', 'aaa', 'abcdefghijasfasfasf' . mt_rand(0, 10000));
    // 
    // var_dump($p->hget('testhash', ['test222', 'abc', 'xxx']));
    // var_dump($p->hgetall('testhash'));
    // var_dump($p->hexists('testhash', 'test'));
    // var_dump($p->hexists('testhash', 'test222'));
    // var_dump($p->hlen('testhash'));
    // var_dump($p->hdel('testhash', array('test')));
    // var_dump($p->hkeys('testhash'));
    // var_dump($p->hvals('testhash'));
    // $p->set('a4', 'test' . mt_rand(0, 1000));
    // $p->set('a6', 'test' . mt_rand(0, 1000));
    // var_dump($p->get('a4'));
    // var_dump($p->rename('a4', 'a6'));
    // var_dump($p->get('a4'));
    // var_dump($p->get('a6'));
    // var_dump($p->ttl('a4'));
    // var_dump($p->presist('a4'));
    // var_dump($p->ttl('a4'));
    // var_dump($p->ttl('a99'));
    // var_dump($p->exists('a6'));
    // var_dump($p->exists('a99'));
    // var_dump($p->expire('a4', 20));
    // echo time();
    // $p->delete('a6');
    // $key = 'mt_'. mt_rand(1, 10000000);
    // $p->set($key, 'test' . mt_rand(0, 1000));
    ////
    // for($i = 0; $i < 200000; $i++) {
//         // $p->set('abcd_'. $i, json_encode($_SERVER));
//         $x = $p->get('abcd_' . $i);
//         if($i % 10000 == 0) 
//             echo $i, "\n";
//         if (!$x) {
//             echo 'error', $i;
//         }
//     }
    // $key = 'l:a:1';
    // var_dump($p->lpush($key, 'test' . mt_rand(0, 1000)));
    // var_dump($p->lrange($key));
    // var_dump($p->lpop($key));
    // var_dump($p->lrange($key));
    // var_dump($p->llen($key));
    // $p->linsert($key, 4, 'test_' . mt_rand(0, 1000));
    // var_dump($p->llen($key));
    // //
    // //var_dump($p->lrange('a2'));
    // $p->delete($key);
    var_dump($p->status());
    // $p->save();
 } catch (Exception $ex) {
     echo $ex->getMessage(), '<br />';
 }
echo sprintf('run is %0.4f ms', (microtime(true) - $start) * 1000);