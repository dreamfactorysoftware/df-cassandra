<?php
namespace DreamFactory\Core\Cassandra\Services;

use DreamFactory\Core\Cassandra\Database\CassandraConnection;
use DreamFactory\Core\Components\DbSchemaExtras;
use DreamFactory\Core\Components\RequireExtensions;
use DreamFactory\Core\Contracts\SchemaInterface;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Services\BaseNoSqlDbService;
use DreamFactory\Core\Cassandra\Resources\Schema;
use DreamFactory\Core\Cassandra\Resources\Table;
use DreamFactory\Core\Contracts\CacheInterface;
use DreamFactory\Core\Contracts\DbExtrasInterface;
use Illuminate\Database\DatabaseManager;
use DreamFactory\Core\Utility\Session;
use DreamFactory\Core\Database\Schema\TableSchema;

class Cassandra extends BaseNoSqlDbService implements CacheInterface, DbExtrasInterface
{
    use DbSchemaExtras, RequireExtensions;

    /** @type CassandraConnection */
    protected $dbConn = null;

    /** @type SchemaInterface */
    protected $schema = null;

    /**
     * @var array
     */
    protected static $resources = [
        Schema::RESOURCE_NAME => [
            'name'       => Schema::RESOURCE_NAME,
            'class_name' => Schema::class,
            'label'      => 'Schema',
        ],
        Table::RESOURCE_NAME  => [
            'name'       => Table::RESOURCE_NAME,
            'class_name' => Table::class,
            'label'      => 'Table',
        ],
    ];

    public function __construct(array $settings)
    {
        parent::__construct($settings);

        $config = array_get($settings, 'config');
        $config = (empty($config) ? [] : (!is_array($config) ? [$config] : $config));
        Session::replaceLookups($config, true);
        $config['driver'] = 'cassandra';

        if (empty($config)) {
            throw new InternalServerErrorException('No service configuration found for Cassandra.');
        }

        // add config to global for reuse, todo check existence and update?
        config(['database.connections.service.' . $this->name => $config]);
        /** @type DatabaseManager $db */
        $db = app('db');
        $this->dbConn = $db->connection('service.' . $this->name);
        $this->schema = new \DreamFactory\Core\Cassandra\Database\Schema\Schema($this->dbConn);

        $this->schema->setCache($this);
        $this->schema->setExtraStore($this);
    }

    /**
     * @throws \Exception
     * @return CassandraConnection
     */
    public function getConnection()
    {
        if (!isset($this->dbConn)) {
            throw new InternalServerErrorException('Database connection has not been initialized.');
        }

        return $this->dbConn;
    }

    /**
     * @throws \Exception
     * @return SchemaInterface
     */
    public function getSchema()
    {
        if (!isset($this->schema)) {
            throw new InternalServerErrorException('Database schema extension has not been initialized.');
        }

        return $this->schema;
    }

    /**
     * @param null $schema
     * @param bool $refresh
     * @param bool $use_alias
     *
     * @return array|TableSchema[]|mixed
     */
    public function getTableNames($schema = null, $refresh = false, $use_alias = false)
    {
        /** @type TableSchema[] $tables */
        $tables = $this->schema->getTableNames($schema, true, $refresh);
        if ($use_alias) {
            $temp = []; // reassign index to alias
            foreach ($tables as $table) {
                $temp[strtolower($table->getName(true))] = $table;
            }

            return $temp;
        }

        return $tables;
    }

    public function refreshTableCache()
    {
        // TODO: Implement refreshTableCache() method.
    }

}