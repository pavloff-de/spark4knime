package de.pavloff.spark4knime.transformations;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "Distinct" Node.
 * Creates a new RDD that contains the distinct elements of the source RDD's
 *
 * @author Oleg Pavlov, University of Heidelberg
 */
public class DistinctNodeFactory 
        extends NodeFactory<DistinctNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public DistinctNodeModel createNodeModel() {
        return new DistinctNodeModel();
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
    public NodeView<DistinctNodeModel> createNodeView(final int viewIndex,
            final DistinctNodeModel nodeModel) {
        return new DistinctNodeView(nodeModel);
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
        return new DistinctNodeDialog();
    }

}

