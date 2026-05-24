/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spi.constraints;

import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

public class ForeignKeyConstraint<T>
        extends TableConstraint<T>
{
    private final SchemaTableName referencedTable;
    private final LinkedHashSet<String> referencedColumns;

    @JsonCreator
    public ForeignKeyConstraint(
            @JsonProperty("name") Optional<String> name,
            @JsonProperty("columns") LinkedHashSet<T> columnNames,
            @JsonProperty("referencedTable") SchemaTableName referencedTable,
            @JsonProperty("referencedColumns") LinkedHashSet<String> referencedColumns,
            @JsonProperty("enabled") boolean enabled,
            @JsonProperty("rely") boolean rely,
            @JsonProperty("enforced") boolean enforced)
    {
        super(name, columnNames, enabled, rely, enforced);
        this.referencedTable = requireNonNull(referencedTable, "referencedTable is null");
        requireNonNull(referencedColumns, "referencedColumns is null");
        if (referencedColumns.isEmpty()) {
            throw new IllegalArgumentException("referencedColumns is empty.");
        }
        if (columnNames.size() != referencedColumns.size()) {
            throw new IllegalArgumentException("foreign key columns and referenced columns must have the same size.");
        }
        this.referencedColumns = new LinkedHashSet<>(referencedColumns);
    }

    @JsonProperty
    public SchemaTableName getReferencedTable()
    {
        return referencedTable;
    }

    @JsonProperty
    public LinkedHashSet<String> getReferencedColumns()
    {
        return referencedColumns;
    }

    @Override
    public <T, R> Optional<TableConstraint<R>> rebaseConstraint(Map<T, R> assignments)
    {
        if (this.getColumns().stream().allMatch(assignments::containsKey)) {
            return Optional.of(new ForeignKeyConstraint<R>(
                    getName(),
                    this.getColumns().stream().map(assignments::get).collect(toCollection(LinkedHashSet::new)),
                    referencedTable,
                    referencedColumns,
                    isEnabled(),
                    isRely(),
                    isEnforced()));
        }
        return Optional.empty();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), referencedTable, referencedColumns);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!super.equals(obj)) {
            return false;
        }
        ForeignKeyConstraint<?> other = (ForeignKeyConstraint<?>) obj;
        return Objects.equals(this.referencedTable, other.referencedTable) &&
                Objects.equals(this.referencedColumns, other.referencedColumns);
    }

    @Override
    public String toString()
    {
        StringBuilder stringBuilder = new StringBuilder(this.getClass().getSimpleName());
        stringBuilder.append(" {");
        stringBuilder.append("name='").append(getName().orElse("null")).append('\'');
        stringBuilder.append(", columns='").append(getColumns()).append('\'');
        stringBuilder.append(", referencedTable='").append(referencedTable).append('\'');
        stringBuilder.append(", referencedColumns='").append(referencedColumns).append('\'');
        stringBuilder.append(", enabled='").append(isEnabled()).append('\'');
        stringBuilder.append(", rely='").append(isRely()).append('\'');
        stringBuilder.append(", enforced='").append(isEnforced()).append('\'');
        stringBuilder.append('}');
        return stringBuilder.toString();
    }
}
