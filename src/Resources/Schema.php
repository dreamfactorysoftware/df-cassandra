<?php
namespace DreamFactory\Core\Cassandra\Resources;

use DreamFactory\Core\Cassandra\Database\CassandraConnection;
use DreamFactory\Core\Cassandra\Services\Cassandra;
use DreamFactory\Core\Contracts\SchemaInterface;
use DreamFactory\Core\Resources\BaseNoSqlDbSchemaResource;
use DreamFactory\Core\Contracts\RequestHandlerInterface;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\NotFoundException;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\RestException;
use DreamFactory\Core\Database\Schema\ColumnSchema;

class Schema extends BaseNoSqlDbSchemaResource
{
    //*************************************************************************
    //	Members
    //*************************************************************************

    /**
     * @var CassandraConnection
     */
    protected $dbConn = null;
    /**
     * @var SchemaInterface
     */
    protected $schema = null;

    //*************************************************************************
    //	Methods
    //*************************************************************************

    /**
     * @param RequestHandlerInterface $parent
     */
    public function setParent(RequestHandlerInterface $parent)
    {
        parent::setParent($parent);

        /** @var Cassandra $parent */
        $this->dbConn = $parent->getConnection();

        /** @var Cassandra $parent */
        $this->schema = $parent->getSchema();
    }

    /**
     * {@inheritdoc}
     */
    public function describeTable($name, $refresh = false)
    {
        $name = (is_array($name) ? array_get($name, 'name') : $name);
        if (empty($name)) {
            throw new BadRequestException('Table name can not be empty.');
        }

        try {
            $table = $this->schema->getTable($name, $refresh);
            if (!$table) {
                throw new NotFoundException("Table '$name' does not exist in the database.");
            }

            $result = $table->toArray();
            $result['access'] = $this->getPermissions($name);

            return $result;
        } catch (RestException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to query database schema.\n{$ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function describeField($table, $field, $refresh = false)
    {
        if (empty($table)) {
            throw new BadRequestException('Table name can not be empty.');
        }

        try {
            $result = $this->describeTableFields($table, $field);

            return array_get($result, 0);
        } catch (RestException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Error describing database table '$table' field '$field'.\n" .
                $ex->getMessage(), $ex->getCode());
        }
    }

    /**
     * @param string                $table_name
     * @param null | string | array $field_names
     * @param bool                  $refresh
     *
     * @throws NotFoundException
     * @throws InternalServerErrorException
     * @return array
     */
    public function describeTableFields($table_name, $field_names = null, $refresh = false)
    {
        $table = $this->schema->getTable($table_name, $refresh);
        if (!$table) {
            throw new NotFoundException("Table '$table_name' does not exist in the database.");
        }

        if (!empty($field_names)) {
            $field_names = static::validateAsArray($field_names, ',', true, 'No valid field names given.');
        }

        $out = [];
        try {
            /** @var ColumnSchema $column */
            foreach ($table->columns as $column) {
                if (empty($field_names) || (false !== array_search($column->name, $field_names))) {
                    $out[] = $column->toArray();
                }
            }
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to query table field schema.\n{$ex->getMessage()}");
        }

        if (empty($out)) {
            throw new NotFoundException("No requested fields found in table '$table_name'.");
        }

        return $out;
    }

    /**
     * {@inheritdoc}
     */
    public function deleteTable($table, $check_empty = false)
    {
        if (empty($table)) {
            throw new BadRequestException('Table name can not be empty.');
        }

        //  Does it exist
        if (!$this->doesTableExist($table)) {
            throw new NotFoundException("Table '$table' not found.");
        }

        try {
            $this->schema->dropTable($table);
        } catch (\Exception $ex) {
            \Log::error('Exception dropping table: ' . $ex->getMessage());

            throw $ex;
        }

        //  Any changes here should refresh cached schema
        $this->refreshCachedTables();
    }

    /**
     * {@inheritdoc}
     */
    public function updateTable($table, $properties, $allow_delete_fields = false, $return_schema = false)
    {
        $properties = (is_array($properties) ? $properties : []);
        $properties['name'] = $table;

        $tables = static::validateAsArray($properties, null, true, 'Bad data format in request.');

        $result = $this->schema->updateSchema($tables, true, $allow_delete_fields);
        $result = array_get($result, 0, []);

        //  Any changes here should refresh cached schema
        $this->refreshCachedTables();

        if ($return_schema) {
            return $this->describeTable($table);
        }

        return $result;
    }

    public function createTable($table, $properties = [], $check_exist = false, $return_schema = false)
    {
        $properties = (is_array($properties) ? $properties : []);
        $properties['name'] = $table;

        $tables = static::validateAsArray($properties, null, true, 'Bad data format in request.');
        $result = $this->schema->updateSchema($tables);
        $result = array_get($result, 0, []);

        //  Any changes here should refresh cached schema
        $this->refreshCachedTables();

        if ($return_schema) {
            return $this->describeTable($table);
        }

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function deleteField($table, $field)
    {
        if (empty($table)) {
            throw new BadRequestException('Table name can not be empty.');
        }

        // does it already exist
        if (!$this->doesTableExist($table)) {
            throw new NotFoundException("A table with name '$table' does not exist in the database.");
        }

        try {
            $this->schema->dropColumn($table, $field);
        } catch (\Exception $ex) {
            error_log($ex->getMessage());
            throw $ex;
        }

        //  Any changes here should refresh cached schema
        $this->refreshCachedTables();
    }
}