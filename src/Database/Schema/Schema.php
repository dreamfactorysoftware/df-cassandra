<?php
namespace DreamFactory\Core\Cassandra\Database\Schema;

use DreamFactory\Core\Cassandra\Database\CassandraConnection;
use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Enums\DbSimpleTypes;

class Schema extends \DreamFactory\Core\Database\Components\Schema
{
    /** @var  CassandraConnection */
    protected $connection;

    /**
     * @inheritdoc
     */
    protected function findColumns(TableSchema $table)
    {
        $cTable = $this->connection->getClient()->getTable($table->name);
        $columns = $cTable->columns();
        $primaryKeys = $cTable->primaryKey();
        $pkNames = [];
        foreach ($primaryKeys as $pk) {
            $pkNames[] = $pk->name();
        }

        $out = [];
        if (!empty($columns)) {
            foreach ($columns as $name => $column) {
                $out[] = [
                    'name'         => $name,
                    'isPrimaryKey' => (in_array($name, $pkNames)) ? true : false,
                    'allowNull'    => true,
                    'type'         => $column->type()->name(),
                    'dbType'       => $column->type()->name(),
                ];
            }
        }

        return $out;
    }

    /**
     * Returns all table names in the database.
     *
     * @param string $schema the schema of the tables. Defaults to empty string, meaning the current or default schema.
     *                       If not empty, the returned table names will be prefixed with the schema name.
     *
     * @return array all table names in the database.
     */
    protected function findTableNames($schema = '')
    {
        $outTables = [];
        $client = $this->connection->getClient();
        $tables = $client->listTables();
        $schemaName = $client->getKeyspace()->name();

        foreach ($tables as $table) {
            $name = array_get($table, 'table_name');
            $cTable = $client->getTable($name);
            $primaryKey = array_get($cTable->primaryKey(), 0);
            $outTables[strtolower($name)] = new TableSchema([
                'schemaName'   => $schemaName,
                'name'         => $name,
                'primaryKey'   => $primaryKey->name()
            ]);
        }

        return $outTables;
    }

    /**
     * @param array $info
     *
     * @return string
     * @throws \Exception
     */
    protected function buildColumnDefinition(array $info)
    {
        // This works for most except Oracle
        $type = (isset($info['type'])) ? $info['type'] : null;
        $typeExtras = (isset($info['type_extras'])) ? $info['type_extras'] : null;

        $definition = $type . $typeExtras;

        if ('string' === $definition) {
            $definition = 'text';
        }

        //$allowNull = (isset($info['allow_null'])) ? $info['allow_null'] : null;
        //$definition .= ($allowNull) ? ' NULL' : ' NOT NULL';

        $default = (isset($info['db_type'])) ? $info['db_type'] : null;
        if (isset($default)) {
            if (is_array($default)) {
                $expression = (isset($default['expression'])) ? $default['expression'] : null;
                if (null !== $expression) {
                    $definition .= ' DEFAULT ' . $expression;
                }
            } else {
                $default = $this->quoteValue($default);
                $definition .= ' DEFAULT ' . $default;
            }
        }

        $isUniqueKey = (isset($info['is_unique'])) ? filter_var($info['is_unique'], FILTER_VALIDATE_BOOLEAN) : false;
        $isPrimaryKey =
            (isset($info['is_primary_key'])) ? filter_var($info['is_primary_key'], FILTER_VALIDATE_BOOLEAN) : false;
        if ($isPrimaryKey && $isUniqueKey) {
            throw new \Exception('Unique and Primary designations not allowed simultaneously.');
        }

        if ($isUniqueKey) {
            $definition .= ' UNIQUE KEY';
        } elseif ($isPrimaryKey) {
            $definition .= ' PRIMARY KEY';
        }

        return $definition;
    }

    /**
     * @inheritdoc
     */
    public function alterColumn($table, $column, $definition)
    {
        if (null !== array_get($definition, 'new_name') &&
            array_get($definition, 'name') !== array_get($definition, 'new_name')
        ) {
            $cql = 'ALTER TABLE ' .
                $table .
                ' RENAME ' .
                $this->quoteColumnName($column) .
                ' TO ' .
                $this->quoteColumnName(array_get($definition, 'new_name'));
        } else {
            $cql = 'ALTER TABLE ' .
                $table .
                ' ALTER ' .
                $this->quoteColumnName($column) .
                ' TYPE ' .
                $this->getColumnType($definition);
        }

        return $cql;
    }

    /**
     * @inheritdoc
     */
    public function dropColumns($table, $columns)
    {
        $columns = (array)$columns;

        if (!empty($columns)) {
            return $this->connection->statement("ALTER TABLE $table DROP " . implode(',', $columns));
        }

        return false;
    }
}