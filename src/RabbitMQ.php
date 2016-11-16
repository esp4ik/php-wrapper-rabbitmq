<?php

namespace wrapper;

use PhpAmqpLib\Connection\AMQPConnection;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use Exception;

class RabbitMQ
{
    /**
     * @var AMQPConnection
     */
    private $connection = null;

    /**
     * @var AMQPChannel
     */
    private $channel = null;

    /**
     * @var string
     */
    public $host = 'localhost';

    /**
     * @var int
     */
    public $port = 5672;

    /**
     * @var string
     */
    public $username = 'guest';

    /**
     * @var string
     */
    public $password = 'guest';

    /**
     * @param string $host
     * @param integer $port
     * @param string $username
     * @param string $password
     */
    public function __construct($host = null, $port = null, $username = null, $password = null)
    {
        $this->host = $host ?: $this->host;
        $this->port = $port ?: $this->port;
        $this->username = $username ?: $this->username;
        $this->password = $password ?: $this->password;
    }

    function __destruct()
    {
        $this->close();
    }

    /**
     * @return AMQPConnection
     * @throws Exception
     */
    protected function getConnection()
    {
        if ($this->connection === null)
        {
            $this->connection = new AMQPConnection($this->host, $this->port, $this->username, $this->password);
        }

        if (empty($this->connection))
        {
            throw new Exception('Fail to connect to RabbitMQ server');
        }

        return $this->connection;
    }

    /**
     * @return AMQPChannel
     * @throws Exception
     */
    protected function getChannel()
    {
        if ($this->channel === null)
        {
            $this->channel = $this->getConnection()->channel();
        }

        if (empty($this->channel))
        {
            throw new Exception('Fail to connect to RabbitMQ server');
        }

        return $this->channel;
    }

    /**
     * @param string $title
     * @param boolean $durable
     * @param boolean $autoDelete
     * @param array $arguments
     * @throws Exception
     */
    public function createQueue($title, $durable = false, $autoDelete = false, $arguments = null)
    {
        $this->getChannel()->queue_declare($title, false, $durable, false, $autoDelete, false, $arguments);
        $this->getChannel()->basic_qos(null, 1, null);
    }

    /**
     * @param string $message
     * @param string $queue
     * @param string $exchange
     * @throws Exception
     */
    public function publishMessage($message, $queue, $exchange = '')
    {
        $message = new AMQPMessage($message);

        $this->getChannel()->basic_publish($message, $exchange, $queue);
    }

    /**
     * @param string $queue
     * @param callable $callback
     * @param string $exchange
     * @param bool $noAck
     * @throws Exception
     */
    public function listen($queue, $callback, $exchange = '', $noAck = false)
    {
        $this->getChannel()->basic_consume($queue, $exchange, false, $noAck, false, false, $callback);

        while(count($this->getChannel()->callbacks))
        {
            $this->getChannel()->wait();
        }
    }

    /**
     * @param $queue
     * @return mixed
     */
    public function getMessage($queue)
    {
        return $this->getChannel()->basic_get($queue)->body;
    }

    public function close()
    {
        if (!empty($this->channel))
        {
            $this->channel->close();
        }

        if (!empty($this->connection))
        {
            $this->connection->close();
        }
    }
}