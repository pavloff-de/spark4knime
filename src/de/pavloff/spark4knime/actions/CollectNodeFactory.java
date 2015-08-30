package de.pavloff.spark4knime.actions;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "Collect" Node.
 * Collect all the elements of the RDD as a table
 *
 * @author Oleg Pavlov, University of Heidelberg
 */
public class CollectNodeFactory 
        extends NodeFactory<CollectNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public CollectNodeModel createNodeModel() {
        return new CollectNodeModel();
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
    public NodeView<CollectNodeModel> createNodeView(final int viewIndex,
            final CollectNodeModel nodeModel) {
        return new CollectNodeView(nodeModel);
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
        return new CollectNodeDialog();
    }

}

