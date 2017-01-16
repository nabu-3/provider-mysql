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

use nabu\core\exceptions\ENabuCoreException;
use nabu\db\CNabuDBAbstractDescriptor;
use nabu\db\exceptions\ENabuDBException;
use nabu\db\interfaces\INabuDBConnector;
use nabu\db\interfaces\INabuDBDescriptor;

/**
 * Class to manage a JSON MySQL Descriptor Storage of a MySQL table.
 * @author Rafael Gutierrez <rgutierrez@wiscot.com>
 * @version 3.0.0 Surface
 * @package providers\mysql
 */
final class CMySQLDescriptor extends CNabuDBAbstractDescriptor
{
    /**
     * Overrides parent constructor to check integrity of the descriptor.
     * @param INabuDBConnector $nb_connector MySQL Database Connector.
     * @param array $storage_descriptor JSON descriptor decoded as associative array.
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
                             . $key_name . '=' . $this->buildFieldValue($this->getField($key_name), $data);
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

    public function buildFieldReplacement($field_descriptor, $alias = false)
    {
        if ($alias === false) {
            $alias = $field_descriptor['name'];
        }

        switch ($field_descriptor['data_type']) {
            case 'int':
                $retval = "%d$alias\$d";
                break;
            case 'varchar':
                $retval = "'%s$alias\$s'";
                break;
            default:
                $retval = false;
        }

        return $retval;
    }

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
                case 'varchar':
                case 'text':
                case 'enum':
                    $retval = $this->nb_connector->buildSentence("'%s'", $value);
                    break;
                case 'datetime':
                    if ($field_descriptor['name'] !== $this->storage_descriptor['name'] . '_creation_datetime') {
                        $retval = $this->nb_connector->buildSentence("'%s'", $value);
                    } else {
                        $retval = false;
                    }
                    break;
                default:
                    throw new ENabuCoreException(ENabuCoreException::ERROR_FEATURE_NOT_IMPLEMENTED);
            }
        }

        return $retval;
    }
}
