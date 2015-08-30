package de.pavloff.spark4knime.transformations;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "Intersection" Node.
 * Create an Intersection of two RDD's
 *
 * @author Oleg Pavlov, University of Heidelberg
 */
public class IntersectionNodeFactory 
        extends NodeFactory<IntersectionNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public IntersectionNodeModel createNodeModel() {
        return new IntersectionNodeModel();
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
    public NodeView<IntersectionNodeModel> createNodeView(final int viewIndex,
            final IntersectionNodeModel nodeModel) {
        return new IntersectionNodeView(nodeModel);
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
        return new IntersectionNodeDialog();
    }

}

