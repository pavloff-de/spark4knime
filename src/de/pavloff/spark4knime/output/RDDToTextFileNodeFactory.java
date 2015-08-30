package de.pavloff.spark4knime.output;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "RDDToTextFile" Node.
 * Save RDD to Text File
 *
 * @author Oleg Pavlov, University of Heidelberg
 */
public class RDDToTextFileNodeFactory 
        extends NodeFactory<RDDToTextFileNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public RDDToTextFileNodeModel createNodeModel() {
        return new RDDToTextFileNodeModel();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNrNodeViews() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeView<RDDToTextFileNodeModel> createNodeView(final int viewIndex,
            final RDDToTextFileNodeModel nodeModel) {
        return new RDDToTextFileNodeView(nodeModel);
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
        return new RDDToTextFileNodeDialog();
    }

}

