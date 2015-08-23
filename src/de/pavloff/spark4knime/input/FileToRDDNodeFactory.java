package de.pavloff.spark4knime.input;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "FileToRDD" Node. Read a text file and
 * parallelize data by lines
 * 
 * @author Oleg Pavlov
 */
public class FileToRDDNodeFactory extends NodeFactory<FileToRDDNodeModel> {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FileToRDDNodeModel createNodeModel() {
		return new FileToRDDNodeModel();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNrNodeViews() {
		// take and visualize first entries of RDD
		return 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NodeView<FileToRDDNodeModel> createNodeView(final int viewIndex,
			final FileToRDDNodeModel nodeModel) {
		return new FileToRDDNodeView(nodeModel);
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
		return new FileToRDDNodeDialog();
	}

}
