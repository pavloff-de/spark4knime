package de.pavloff.spark4knime.transformations;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "GroupByKey" Node.
 * When called on a dataset of (K, V) pairs, returns a dataset of (K, Iterable<V>) pairs
 *
 * @author Oleg Pavlov
 */
public class GroupByKeyNodeFactory 
        extends NodeFactory<GroupByKeyNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public GroupByKeyNodeModel createNodeModel() {
        return new GroupByKeyNodeModel();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNrNodeViews() {
        return 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeView<GroupByKeyNodeModel> createNodeView(final int viewIndex,
            final GroupByKeyNodeModel nodeModel) {
        return new GroupByKeyNodeView(nodeModel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasDialog() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeDialogPane createNodeDialogPane() {
        return new GroupByKeyNodeDialog();
    }

}

