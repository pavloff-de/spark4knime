package de.pavloff.spark4knime.transformations;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "Sample" Node.
 * Sample operation on Spark RDD
 *
 * @author Oleg Pavlov, University of Heidelberg
 */
public class SampleNodeFactory 
        extends NodeFactory<SampleNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public SampleNodeModel createNodeModel() {
        return new SampleNodeModel();
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
    public NodeView<SampleNodeModel> createNodeView(final int viewIndex,
            final SampleNodeModel nodeModel) {
        return new SampleNodeView(nodeModel);
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
        return new SampleNodeDialog();
    }

}

