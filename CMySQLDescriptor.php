<?php

/** @license
 *  Copyright 2009-2011 Rafael Gutierrez Martinez
 *  Copyright 2012-2013 Welma WEB MKT LABS, S.L.
 *  Copyright 2014-2016 Where Ideas Simply Come True, S.L.
 *  Copyright 2017 nabu-3 Group
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

namespace providers\mysql\driver;

use nabu\core\exceptions\ENabuCoreException;
use nabu\db\CNabuDBAbstractDescriptor;
use nabu\db\exceptions\ENabuDBException;
use nabu\db\interfaces\INabuDBConnector;

/**
 * Class to manage a JSON MySQL Descriptor Storage of a MySQL table.
 * @author Rafael Gutierrez <rgutierrez@nabu-3.com>
 * @since 0.0.1
 * @version 0.0.1
 * @package providers\mysql\driver
 */
final class CMySQLDescriptor extends CNabuDBAbstractDescriptor
{
    /**
     * Overrides parent constructor to check integrity of the descriptor.
     * @param INabuDBConnector $nb_connector MySQL Database Connector.
     * @param array|null $storage_descriptor JSON descriptor decoded as associative array.
     */
    public function __construct(INabuDBConnector $nb_connector, array $storage_descriptor = null)
    {
        parent::__construct($nb_connector, $storage_descriptor);

        if (!$this->validateDescriptor()) {
            throw new ENabuDBException(ENabuDBException::ERROR_STORAGE_DESCRIPTOR_SYNTAX_ERROR);
        }
    }

    protected function validateDescriptor($storage_descriptor = false)
    {
        if (!$storage_descriptor) {
            $storage_descriptor = $this->storage_descriptor;
        }

        return
            is_array($storage_descriptor) &&
            array_key_exists('schema', $storage_descriptor) &&
            array_key_exists('name', $storage_descriptor) &&
            array_key_exists('engine', $storage_descriptor) &&
            array_key_exists('type', $storage_descriptor) &&
            array_key_exists('fields', $storage_descriptor) &&
            is_array($storage_descriptor['fields']) &&
            count($storage_descriptor['fields']) > 0 &&
            (!array_key_exists('constraints', $storage_descriptor) || count($storage_descriptor['constraints']) > 0)
        ;
    }

    protected function buildDocumentIdentifierFilterDefault()
    {
        $this->getPrimaryConstraint();
        if (is_array($this->primary_constraint) &&
            array_key_exists('fields', $this->primary_constraint) &&
            count($this->primary_constraint['fields']) > 0
        ) {
            $filter = '';
            foreach ($this->primary_constraint['fields'] as $key_name => $key_field) {
                if (is_array($key_field)) {
                    $filter .= (strlen($filter) > 0 ? ' and ' : '')
                             . $key_name . '=' . $this->buildFieldReplacement($this->getField($key_name));
                } else {
                    throw new ENabuDBException(ENabuDBException::ERROR_STORAGE_DESCRIPTOR_SYNTAX_ERROR);
                }
            }

            return $filter;
        } else {
            throw new ENabuDBException(
                ENabuDBException::ERROR_PRIMARY_CONSTRAINT_NOT_EXISTS,
                0, null, null, array($this->storage_descriptor['schema'] . '.' . $this->storage_descriptor['name'])
            );
        }
    }

    protected function buildDocumentIdentifierFilterData($data)
    {
        $this->getPrimaryConstraint();
        if (is_array($data) &&
            count($data) > 0 &&
            is_array($this->primary_constraint) &&
            array_key_exists('fields', $this->primary_constraint) &&
            count($this->primary_constraint['fields']) > 0
        ) {
            $filter = '';
            foreach ($this->primary_constraint['fields'] as $key_name => $key_field) {
                if (is_array($key_field) && array_key_exists($key_name, $data)) {
                    $filter .= (strlen($filter) > 0 ? ' and ' : '')
                             . $key_name . '=' . $this->buildFieldValue($this->getField($key_name), $data[$key_name]);
                } else {
                    throw new ENabuDBException(ENabuDBException::ERROR_STORAGE_DESCRIPTOR_SYNTAX_ERROR);
                }
            }

            return $filter;
        } else {
            throw new ENabuDBException(
                ENabuDBException::ERROR_PRIMARY_CONSTRAINT_NOT_EXISTS,
                 0, null, null, array($this->storage_descriptor['schema'] . '.' . $this->storage_descriptor['name'])
            );
        }
    }

    /**
     * Builds a well formed replacement string for a field value.
     * @param array $field_descriptor Field descriptor to build the replacement string.
     * @param string $alias Alternate name used to map the value in the replacement string.
     * @return string Returns a well formed replacement string of false if no valid descriptor data_type found in
     * field descriptor.
     */
    public function buildFieldReplacement(array $field_descriptor, $alias = false)
    {
        if ($alias === false) {
            $alias = $field_descriptor['name'];
        }

        switch ($field_descriptor['data_type']) {
            case 'int':
                $retval = "%d$alias\$d";
                break;
            case 'float':
            case 'double':
                $retval = "%F$alias\$d";
                break;
            case 'varchar':
            case 'text':
            case 'longtext':
            case 'enum':
            case 'set':
            case 'tinytext':
                $retval = "'%s$alias\$s'";
                break;
            default:
                $retval = false;
        }

        return $retval;
    }

    /**
     * Builds a well formed string for a field value containing their value represented in MySQL SQL syntax.
     * This method prevents SQL Injection.
     * @param array $field_descriptor Field descriptor to build the field value string.
     * @param mixed $value Value to be converted into a valid MySQL value representation.
     * @return string Returns the well formed string or false if no valid data_type found in field descriptor.
     */
    public function buildFieldValue($field_descriptor, $value)
    {
        if ($value === null) {
            $retval = 'NULL';
        } elseif ($value === false) {
            $retval = 'false';
        } elseif ($value === true) {
            $retval = 'true';
        } else {
            if (!is_array($value)) {
                $value = array($value);
            }
            switch ($field_descriptor['data_type']) {
                case 'int':
                    $retval = $this->nb_connector->buildSentence('%d', $value);
                    break;
                case 'float':
                case 'double':
                    $retval = $this->nb_connector->buildSentence('%F', $value);
                    break;
                case 'varchar':
                case 'text':
                case 'longtext':
                case 'enum':
                case 'set':
                case 'tinytext':
                    $retval = $this->nb_connector->buildSentence("'%s'", $value);
                    break;
                case 'date':
                case 'datetime':
                    if ($field_descriptor['name'] !== $this->storage_descriptor['name'] . '_creation_datetime') {
                        if ($value === null) {
                            $retval = 'null';
                        } else {
                            $retval = $this->nb_connector->buildSentence("'%s'", $value);
                        }
                    } else {
                        $retval = false;
                    }
                    break;
                default:
                    error_log($field_descriptor['data_type']);
                    throw new ENabuCoreException(ENabuCoreException::ERROR_FEATURE_NOT_IMPLEMENTED);
            }
        }

        return $retval;
    }
}
