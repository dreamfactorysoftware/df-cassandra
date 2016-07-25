<?php
namespace DreamFactory\Core\Cassandra\Database\Schema;

use DreamFactory\Core\Cassandra\Database\CassandraConnection;
use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Enums\DbSimpleTypes;

class Schema extends \DreamFactory\Core\Database\Schema\Schema
{
    /** @var  CassandraConnection */
    protected $connection;

    /**
     * @inheritdoc
     */
    protected function loadTable(TableSchema $table)
    {
        $cTable = $this->connection->getClient()->getTable($table->name);
        $columns = $cTable->columns();
        $primaryKeys = $cTable->primaryKey();
        $pkNames = [];
        foreach ($primaryKeys as $pk) {
            $pkNames[] = $pk->name();
        }

        if (!empty($columns)) {
            foreach ($columns as $name => $column) {
                $c = new ColumnSchema([
                    'name'         => $name,
                    'isPrimaryKey' => (in_array($name, $pkNames)) ? true : false,
                    'allowNull'    => true,
                    'type'         => $column->type()->name(),
                    'dbType'       => $column->type()->name(),

                ]);
                $table->addColumn($c);
            }
        }

        return $table;
    }

    /**
     * @param ColumnSchema $field
     * @param bool         $as_quoted_string
     *
     * @return \Illuminate\Database\Query\Expression|string
     */
    public function parseFieldForFilter(ColumnSchema $field, $as_quoted_string = false)
    {
        return $field->name;
//        switch ($field->dbType) {
//            case null:
//                return DB::raw($field->getDbFunction());
//        }
//
//        return ($as_quoted_string) ? $field->rawName : $field->name;
    }

    /**
     * @param \DreamFactory\Core\Database\Schema\ColumnSchema $column
     *
     * @return array
     */
    public function getPdoBinding(ColumnSchema $column)
    {
        switch ($column->dbType) {
            case null:
                $type = $column->getDbFunctionType();
                $pdoType = $this->extractPdoType($type);
                $phpType = $type;
                break;
            default:
                $pdoType = ($column->allowNull) ? null : $column->pdoType;
                $phpType = $column->phpType;
                break;
        }

        return ['name' => $column->getName(true), 'pdo_type' => $pdoType, 'php_type' => $phpType];
    }

    /**
     * @param $value
     * @param $field_info
     *
     * @return mixed
     */
    public function parseValueForSet($value, $field_info)
    {
        switch ($field_info->dbType) {
            case 'int':
                return intval($value);
            default:
                return $value;
        }
    }

    /**
     * @param ColumnSchema $field
     * @param bool         $as_quoted_string
     *
     * @return \Illuminate\Database\Query\Expression|string
     */
    public function parseFieldForSelect(ColumnSchema $field, $as_quoted_string = false)
    {
        switch ($field->dbType) {
            //case null:
            //    return DB::raw($field->getDbFunction() . ' AS ' . $this->quoteColumnName($field->getName(true)));
            default :
                $out = ($as_quoted_string) ? $field->rawName : $field->name;
                if (!empty($field->alias)) {
                    $out .= ' AS ' . $field->alias;
                }

                return $out;
        }
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
        $schemaName = $client->getKeyspace()->name();

        foreach ($tables as $table) {
            $name = array_get($table, 'table_name');
            $cTable = $client->getTable($name);
            $primaryKey = array_get($cTable->primaryKey(), 0);
            $outTables[strtolower($name)] = new TableSchema([
                'schemaName' => $schemaName,
                'tableName'  => $name,
                'name'       => $name,
                'primaryKey' => $primaryKey->name()
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

        if ('string' === $definition) {
            $definition = 'text';
        }

        return $definition;
    }

    /**
     * Builds a SQL statement for changing the definition of a column.
     *
     * @param string $table      the table whose column is to be changed. The table name will be properly quoted by the
     *                           method.
     * @param string $column     the name of the column to be changed. The name will be properly quoted by the method.
     * @param string $definition the new column type. The {@link getColumnType} method will be invoked to convert
     *                           abstract column type (if any) into the physical one. Anything that is not recognized
     *                           as abstract type will be kept in the generated SQL. For example, 'string' will be
     *                           turned into 'varchar(255)', while 'string not null' will become 'varchar(255) not
     *                           null'.
     *
     * @return string the SQL statement for changing the definition of a column.
     * @since 1.1.6
     */
    public function alterColumn($table, $column, $definition)
    {
        if (null !== array_get($definition, 'new_name') &&
            array_get($definition, 'name') !== array_get($definition, 'new_name')
        ) {
            $cql = 'ALTER TABLE ' .
                $this->quoteTableName($table) .
                ' RENAME ' .
                $this->quoteColumnName($column) .
                ' TO ' .
                $this->quoteColumnName(array_get($definition, 'new_name'));
        } else {
            $cql = 'ALTER TABLE ' .
                $this->quoteTableName($table) .
                ' ALTER ' .
                $this->quoteColumnName($column) .
                ' TYPE ' .
                $this->getColumnType($definition);
        }

        return $cql;
    }

    /**
     * Builds and executes a SQL statement for dropping a DB table.
     *
     * @param string $table the table to be dropped. The name will be properly quoted by the method.
     *
     * @return integer 0 is always returned. See {@link http://php.net/manual/en/pdostatement.rowcount.php} for more
     *                 information.
     */
    public function dropTable($table)
    {
        $sql = "DROP TABLE " . $this->quoteTableName($table);
        $result = $this->connection->statement($sql);
        $this->removeSchemaExtrasForTables($table);

        //  Any changes here should refresh cached schema
        $this->refresh();

        return $result;
    }

    /**
     * @param $table
     * @param $column
     *
     * @return bool|int
     */
    public function dropColumn($table, $column)
    {
        $result = 0;
        $tableInfo = $this->getTable($table);
        if (($columnInfo = $tableInfo->getColumn($column)) && (DbSimpleTypes::TYPE_VIRTUAL !== $columnInfo->type)) {
            $sql = "ALTER TABLE " . $this->quoteTableName($table) . " DROP " . $this->quoteColumnName($column);
            $result = $this->connection->statement($sql);
        }
        $this->removeSchemaExtrasForFields($table, $column);

        //  Any changes here should refresh cached schema
        $this->refresh();

        return $result;
    }
}