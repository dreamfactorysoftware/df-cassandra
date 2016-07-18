<?php
namespace DreamFactory\Core\Cassandra\Database\Schema;

use DreamFactory\Core\Cassandra\Components\Connection;
use DreamFactory\Core\Database\Schema\TableSchema;

class Schema extends \DreamFactory\Core\Database\Schema\Schema
{
    /** @var  Connection */
    protected $connection;
    
    protected function loadTable(TableSchema $table)
    {
        // TODO: Implement loadTable() method.
    }

    /**
     * Returns all table names in the database.
     *
     * @param string $schema the schema of the tables. Defaults to empty string, meaning the current or default schema.
     *                       If not empty, the returned table names will be prefixed with the schema name.
     * @param bool   $include_views
     *
     * @return array all table names in the database.
     */
    protected function findTableNames($schema = '', $include_views = true)
    {
        $outTables = [];
        $client = $this->connection->getClient();
        $tables = $client->listTables();
        $schema = $client->getKeyspace();

        foreach ($tables as $table) {
            $name = array_get($table, 'table_name');
            $outTables[strtolower($name)] = new TableSchema([
                'schemaName' => $schema,
                'tableName'  => $name,
                'name'       => $name,
                'primaryKey' => '_id',
            ]);
        }

        return $outTables;
    }
}