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
package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class ConstraintSpecification
        extends TableElement
{
    public enum ConstraintType
    {
        FOREIGN_KEY,
        UNIQUE,
        PRIMARY_KEY
    }

    private final Optional<String> constraintName;
    private final List<String> columns;
    private final ConstraintType constraintType;
    private final Optional<QualifiedName> referencedTable;
    private final List<String> referencedColumns;
    private final boolean enabled;
    private final boolean rely;
    private final boolean enforced;

    public ConstraintSpecification(Optional<String> constraintName, List<String> columns, ConstraintType constraintType, boolean enabled, boolean rely, boolean enforced)
    {
        this(Optional.empty(), constraintName, columns, constraintType, Optional.empty(), ImmutableList.of(), enabled, rely, enforced);
    }

    public ConstraintSpecification(NodeLocation location, Optional<String> constraintName, List<String> columns, ConstraintType constraintType, boolean enabled, boolean rely, boolean enforced)
    {
        this(Optional.of(location), constraintName, columns, constraintType, Optional.empty(), ImmutableList.of(), enabled, rely, enforced);
    }

    public ConstraintSpecification(Optional<String> constraintName, List<String> columns, QualifiedName referencedTable, List<String> referencedColumns, boolean enabled, boolean rely, boolean enforced)
    {
        this(Optional.empty(), constraintName, columns, ConstraintType.FOREIGN_KEY, Optional.of(referencedTable), referencedColumns, enabled, rely, enforced);
    }

    public ConstraintSpecification(NodeLocation location, Optional<String> constraintName, List<String> columns, QualifiedName referencedTable, List<String> referencedColumns, boolean enabled, boolean rely, boolean enforced)
    {
        this(Optional.of(location), constraintName, columns, ConstraintType.FOREIGN_KEY, Optional.of(referencedTable), referencedColumns, enabled, rely, enforced);
    }

    private ConstraintSpecification(Optional<NodeLocation> location, Optional<String> constraintName, List<String> columns, ConstraintType constraintType, Optional<QualifiedName> referencedTable, List<String> referencedColumns, boolean enabled, boolean rely, boolean enforced)
    {
        super(location);
        this.constraintName = requireNonNull(constraintName, "constraint name is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "constraint columns is null"));
        this.constraintType = requireNonNull(constraintType, "constraintType is null");
        this.referencedTable = requireNonNull(referencedTable, "referencedTable is null");
        this.referencedColumns = ImmutableList.copyOf(requireNonNull(referencedColumns, "referencedColumns is null"));
        if (constraintType == ConstraintType.FOREIGN_KEY) {
            if (!referencedTable.isPresent()) {
                throw new IllegalArgumentException("referencedTable is missing for foreign key");
            }
            if (referencedColumns.isEmpty()) {
                throw new IllegalArgumentException("referencedColumns is empty for foreign key");
            }
            if (columns.size() != referencedColumns.size()) {
                throw new IllegalArgumentException("foreign key columns and referenced columns must have the same size");
            }
        }
        else if (referencedTable.isPresent() || !referencedColumns.isEmpty()) {
            throw new IllegalArgumentException("referenced table and columns are only valid for foreign key constraints");
        }
        this.enabled = enabled;
        this.rely = rely;
        this.enforced = enforced;
    }

    public Optional<String> getConstraintName()
    {
        return constraintName;
    }

    public List<String> getColumns()
    {
        return columns;
    }

    public ConstraintType getConstraintType()
    {
        return constraintType;
    }

    public Optional<QualifiedName> getReferencedTable()
    {
        return referencedTable;
    }

    public List<String> getReferencedColumns()
    {
        return referencedColumns;
    }

    public boolean isRely()
    {
        return rely;
    }

    public boolean isEnabled()
    {
        return enabled;
    }

    public boolean isEnforced()
    {
        return enforced;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitConstraintSpecification(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ConstraintSpecification o = (ConstraintSpecification) obj;
        return Objects.equals(constraintName, o.constraintName) &&
                Objects.equals(columns, o.columns) &&
                Objects.equals(constraintType, o.constraintType) &&
                Objects.equals(referencedTable, o.referencedTable) &&
                Objects.equals(referencedColumns, o.referencedColumns) &&
                Objects.equals(rely, o.rely) &&
                Objects.equals(enabled, o.enabled) &&
                Objects.equals(enforced, o.enforced);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(constraintName, columns, constraintType, referencedTable, referencedColumns, rely, enabled, enforced);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("constraintName", constraintName)
                .add("columns", columns)
                .add("constraintType", constraintType)
                .add("referencedTable", referencedTable)
                .add("referencedColumns", referencedColumns)
                .add("rely", rely)
                .add("enabled", enabled)
                .add("enforced", enforced)
                .toString();
    }
}
