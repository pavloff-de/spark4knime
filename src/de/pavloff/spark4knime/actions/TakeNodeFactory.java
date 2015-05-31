package de.pavloff.spark4knime.actions;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "Take" Node.
 * Take first n elements of RDD
 *
 * @author Oleg Pavlov
 */
public class TakeNodeFactory 
        extends NodeFactory<TakeNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public TakeNodeModel createNodeModel() {
        return new TakeNodeModel();
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
    public NodeView<TakeNodeModel> createNodeView(final int viewIndex,
            final TakeNodeModel nodeModel) {
        return new TakeNodeView(nodeModel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasDialog() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeDialogPane createNodeDialogPane() {
        return new TakeNodeDialog();
    }

}

