<?php

/*  Copyright 2009-2011 Rafael Gutierrez Martinez
 *  Copyright 2012-2013 Welma WEB MKT LABS, S.L.
 *  Copyright 2014-2016 Where Ideas Simply Come True, S.L.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

namespace providers\mysql;

use \nabu\core\exceptions\ENabuCoreException;
use \nabu\db\interfaces\INabuDBSyntaxBuilder;

/**
 * @author Rafael Gutierrez <rgutierrez@wiscot.com>
 * @version 3.0.0 Surface
 * @package providers\mysql
 */
class CMySQLSyntaxBuilder implements INabuDBSyntaxBuilder
{
    /**
     * MySQL Connector instance
     * @var CMySQLConnector
     */
    private $connector = null;

    /**
     * Constructor
     * @param CMySQLConnector $connector MySQL Connector
     * @throws ENabuCoreException
     */
    public function __construct(CMySQLConnector $connector)
    {
        if ($connector === null) {
            throw new ENabuCoreException(ENabuCoreException::ERROR_CONSTRUCTOR_PARAMETER_IS_EMPTY, array('$connector'));
        }

        $this->connector = $connector;
    }

    /**
     * Creates the descriptor from an existent MySQL storage.
     * @param string $name Table name.
     * @param string $schema Table schema.
     * @return CMySQLDescriptor Returns a descriptor instance.
     */
    public function describeStorage($name, $schema = false)
    {
        if (!$schema) {
            $schema = $this->connector->getSchema();
        }

        return $this->describeTable($name, $schema);
    }

    /**
     * Creates the descriptor from an existent MySQL table.
     * @param string $name Table name.
     * @param string $schema Table schema.
     * @return CMySQLDescriptor Returns a descriptor instance or null if no descriptor available.
     */
    private function describeTable($name, $schema)
    {
        $data = $this->connector->getQueryAsSingleRow(
                "select t.table_schema, t.table_name, t.engine, t.auto_increment, "
                     . "t.table_collation, t.table_comment, cs.character_set_name as table_charset "
                . "from information_schema.tables t, information_schema.character_sets cs "
               . "where t.table_schema='%schema\$s' "
                 . "and t.table_name = '%table\$s' "
                . "and t.table_collation = cs.default_collate_name",
                array(
                    'schema' => $schema,
                    'table' => $name
                )
        );

        if (is_array($data)) {
            $table['schema'] = $data['table_schema'];
            $table['name'] = $data['table_name'];
            $table['engine'] = $data['engine'];
            $table['type'] = CMySQLConnector::TYPE_TABLE;
            $table['autoincrement'] = $data['auto_increment'];
            $table['charset'] = $data['table_charset'];
            $table['collation'] = $data['table_collation'];
            $table['comment'] = $data['table_comment'];
            $table['fields'] = $this->describeTableFields($name, $schema);
            $table['constraints'] = $this->describeTableConstraints($name, $table['fields'], $schema);
            $descriptor = new CMySQLDescriptor($this->connector, $table);
        } else {
            $descriptor = null;
        }

        return $descriptor;
    }

    private function describeTableFields($table, $schema)
    {
        $data = $this->connector->getQueryAsAssoc(
                'column_name',
                "select column_name, ordinal_position, data_type, numeric_precision, "
                     . "column_type, column_key, column_default, is_nullable, extra, "
                     . "column_comment "
                . "from information_schema.columns "
               . "where table_schema='%schema\$s' "
                 . "and table_name = '%table\$s'"
              . " order by ordinal_position",
                array(
                    'schema' => $schema,
                    'table' => $table
                )
        );

        if (count($data) > 0) {
            $fields = array();
            foreach ($data as $field) {
                $fields[$field['column_name']] = array(
                    'name' => $field['column_name'],
                    'ordinal' => $field['ordinal_position'],
                    'data_type' => $field['data_type'],
                    'precision' => $field['numeric_precision'],
                    'type' => $field['column_type'],
                    'default' => $field['column_default'],
                    'is_nullable' => ($field['is_nullable'] === 'YES'),
                    'extra' => $field['extra'],
                    'comment' => $field['column_comment']
                );
            }

            return $fields;
        }

        return null;
    }

    private function getTableConstraintPrimaryName($table, $schema)
    {
        return $this->connector->getQueryAsSingleField(
                'constraint_name',
                "select kcu.constraint_name "
                . "from information_schema.columns c, information_schema.key_column_usage kcu "
               . "where c.table_schema='%schema\$s' "
                 . "and c.table_name='%table\$s' "
                 . "and c.table_schema=kcu.table_schema "
                 . "and c.table_name=kcu.table_name "
                 . "and c.column_name=kcu.column_name "
                 . "and c.column_key='PRI' "
               . "group by c.table_schema, c.table_name, kcu.constraint_name, c.column_key",
                array(
                    'schema' => $schema,
                    'table' => $table
                )
        );
    }

    private function describeTableConstraints($table, $fields, $schema, $primary_name = false)
    {
        $constraints = array();
        $primary = array();
        if (!is_string($primary_name)) {
            $primary_name = $this->getTableConstraintPrimaryName($table, $schema);
        }

        $show_keys = $this->connector->getQueryAsArray(
                "show keys from `%schema\$s`.`%table\$s`",
                array(
                    'schema' => $schema,
                    'table' => $table
                )
        );

        if (count($show_keys) > 0) {
            foreach ($show_keys as $field) {
                $key_name = $field['Key_name'];
                if ($key_name == $primary_name) {
                    if (!array_key_exists('name', $primary)) {
                        $primary['name']  = $key_name;
                        $primary['primary'] = true;
                    }
                    if (!array_key_exists('index_type', $primary)) {
                        $primary['index_type'] = $field['Index_type'];
                    }
                    if (!array_key_exists('comments', $primary)) {
                        $primary['comments'] = $field['Index_comment'];
                    }
                    if (!array_key_exists('fields', $primary)) {
                        $primary['fields'] = array();
                    }
                    $primary['fields'][$field['Column_name']] = array(
                        'name' => $field['Column_name'],
                        'ordinal' => $field['Seq_in_index'],
                        'collation' => ($field['Collation'] === 'A' ? 'ASC' : 'DES'),
                        'comments' => $field['Comment']
                    );
                } else {
                    if (!array_key_exists($key_name, $constraints)) {
                        $constraints[$key_name] = array(
                            'name' => $key_name,
                            'primary' => false
                        );
                    }
                    $index = &$constraints[$key_name];
                    if (!array_key_exists('unique', $index)) {
                        $index['unique'] = ($field['Non_unique'] === '0');
                    }
                    if (!array_key_exists('index_type', $index)) {
                        $index['index_type'] = $field['Index_type'];
                    }
                    if (!array_key_exists('fields', $index)) {
                        $index['fields'] = array();
                    }
                    $index['fields'][$field['Column_name']] = array(
                        'name' => $field['Column_name'],
                        'ordinal' => $field['Seq_in_index'],
                        'collation' => ($field['Collation'] === 'A' ? 'ASC' : 'DES'),
                        'null' => ($field['Null'] === 'YES'),
                        'subpart' => $field['Sub_part'],
                        'comments' => $field['Comment']
                    );
                }
            }
        }

        if (count($primary) > 0) {
            $primary = array( $primary['name'] => $primary );
            if (count($constraints) === 0) {
                $constraints = $primary;
            } else {
                $constraints = array_merge($primary, $constraints);
            }
        }

        return count($constraints) > 0 ? $constraints : null;
    }

    public function buildStorageCreationSentence($descriptor, $safe = true)
    {
        if (is_array($descriptor) && array_key_exists('type', $descriptor)) {
            switch ($descriptor['type']) {
                case CMySQLConnector::TYPE_TABLE:
                    return $this->buildTableCreationSentence($descriptor, $safe);
            }
        }

        return false;
    }

    private function buildTableCreationSentence($descriptor, $safe = true)
    {
        if (!array_key_exists('name', $descriptor)) {
            throw new EMySQLException(EMySQLException::ERROR_SYNTAX_TABLE_NAME_NOT_FOUND);
        }

        if (!array_key_exists('fields', $descriptor) || count($descriptor['fields']) === 0) {
            throw new EMySQLException(EMySQLException::ERROR_SYNTAX_FIELDS_NOT_FOUND);
        }

        $parts = array();
        foreach ($descriptor['fields'] as $field) {
            $parts[] = $this->buildFieldCreationFragment($field);
        }

        if (array_key_exists('constraints', $descriptor) && count($descriptor['constraints']) > 0) {
            foreach ($descriptor['constraints'] as $constraint) {
                $parts[] = $this->buildConstraintCreationFragment($constraint);
            }
        }

        $sentence = "create table" . ($safe ? ' if not exists' : '') . " `$descriptor[name]` (\n"
                . implode(",\n", $parts)
                . "\n) engine=$descriptor[engine]"
                . (array_key_exists('autoincrement', $descriptor) &&
                   is_numeric($descriptor['autoincrement']) &&
                   $descriptor['autoincrement'] > 1
                   ? " auto_increment=$descriptor[autoincrement]"
                   : ''
                  )
                . (array_key_exists('charset', $descriptor) ? " default charset=$descriptor[charset]" : '')
                . (array_key_exists('collation', $descriptor) ? " default collate=$descriptor[collation]" : '');

        return $sentence;
    }

    private function buildFieldCreationFragment($field)
    {
        if (!array_key_exists('name', $field)) {
            throw new EMySQLException(EMySQLException::ERROR_SYNTAX_FIELD_NAME_NOT_FOUND);
        }

        $sentence = "`$field[name]`";

        if ($field['data_type'] === 'int' && $field['type'] === 'int(11)') {
            $sentence .= " int";
        } else {
            $sentence .= " " . $field['type'];
        }

        $nullable = (!array_key_exists('is_nullable', $field) || $field['is_nullable']);
        $is_default = (array_key_exists('default', $field) ? true : false);
        $default = (array_key_exists('default', $field) ? $field['default'] : null);

        if (!$nullable) {
            $sentence .= " not null";
            if ($is_default && $default === null) {
                $is_default = false;
            }
        }

        if ($is_default) {
            if ($default === null) {
                $sentence .= " default null";
            } elseif ($default === false) {
                $sentence .= " default false";
            } elseif ($default === true) {
                $sentence .= " default true";
            } elseif (is_numeric($default)) {
                $sentence .= $this->connector->buildSentence(" default %d", array($default));
            } elseif (is_string($default)) {
                $sentence .= $this->connector->buildSentence(" default '%s'", array($default));
            }
        }

        if (array_key_exists('extra', $field) && strlen($field['extra']) > 0) {
            $sentence .= ' ' . $field['extra'];
        }

        return $sentence;
    }

    private function buildConstraintCreationFragment($constraint)
    {
        if (!is_array($constraint)) {
            throw new EMySQLException(EMySQLException::ERROR_SYNTAX_INVALID_CONSTRAINT_DESCRIPTOR);
        }

        if (!array_key_exists('primary', $constraint)) {
            throw new EMySQLException(
                EMySQLException::ERROR_SYNTAX_CONSTRAINT_ATTR_NOT_FOUND,
                0, null, null, array('primary')
            );
        }

        if ($constraint['primary']) {
            return $this->buildConstraintPrimaryCreationFragment($constraint);
        } else {
            return $this->buildConstraintIndexCreationFragment($constraint);
        }
    }

    private function buildConstraintPrimaryCreationFragment($constraint)
    {
        if (array_key_exists('name', $constraint)) {
            $name = $constraint['name'];
        } else {
            $name = 'PRIMARY';
        }

        if (!array_key_exists('fields', $constraint)) {
            throw new EMySQLException(EMySQLException::ERROR_SYNTAX_CONSTRAINT_FIELDS_NOT_FOUND, array($name));
        }

        if (count($constraint['fields']) === 0) {
            throw new EMySQLException(EMySQLException::ERROR_SYNTAX_CONSTRAINT_FIELDS_EMPTY, array($name));
        }

        $fields = implode ('`, `', array_keys($constraint['fields']));

        return "primary key (`$fields`)";
    }

    private function buildConstraintIndexCreationFragment($constraint)
    {
        if (!array_key_exists('name', $constraint)) {
            throw new EMySQLException(EMySQLException::ERROR_SYNTAX_CONSTRAINT_ATTR_NOT_FOUND, array('name'));
        } else {
            $name = $constraint['name'];
        }

        if (!array_key_exists('unique', $constraint)) {
            throw new EMySQLException(EMySQLException::ERROR_SYNTAX_CONSTRAINT_ATTR_NOT_FOUND, array('unique'));
        } else {
            $unique = $constraint['unique'];
        }

        if (array_key_exists('index_type', $constraint)) {
            $type = $constraint['index_type'];
        } else {
            $type = false;
        }

        if (!array_key_exists('fields', $constraint)) {
            throw new EMySQLException(EMySQLException::ERROR_SYNTAX_CONSTRAINT_FIELDS_NOT_FOUND, array($name));
        }

        if (count($constraint['fields']) === 0) {
            throw new EMySQLException(EMySQLException::ERROR_SYNTAX_CONSTRAINT_FIELDS_EMPTY, array($name));
        }

        $fields_arr = array();
        foreach ($constraint['fields'] as $key => $field) {
            $fields_arr[] =
                    "`$key`"
                  . (array_key_exists('subpart', $field) && is_numeric($field['subpart']) ? "($field[subpart])" : '')
                  . (array_key_exists('collation', $field) &&
                     is_string($field['collation'])
                     ? ' ' . $field['collation']
                     : ''
                    )
            ;
        }
        $fields = implode (', ', $fields_arr);

        return ($unique ? 'unique ' : '') . "key `$name` ($fields)";
    }
}
