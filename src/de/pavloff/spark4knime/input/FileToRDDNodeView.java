package de.pavloff.spark4knime.input;

import org.knime.core.node.NodeView;

/**
 * <code>NodeView</code> for the "FileToRDD" Node. Creates view and show first
 * entries of an JavaRDD object.
 * 
 * @author Oleg Pavlov
 */
public class FileToRDDNodeView extends NodeView<FileToRDDNodeModel> {

	/**
	 * Creates a new view.
	 * 
	 * @param nodeModel
	 *            The model (class: {@link FileToRDDNodeModel})
	 */
	protected FileToRDDNodeView(final FileToRDDNodeModel nodeModel) {
		super(nodeModel);

		// TODO: take and visualize first entries of RDD
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void modelChanged() {

		// TODO retrieve the new model from your nodemodel and
		// update the view.
		FileToRDDNodeModel nodeModel = (FileToRDDNodeModel) getNodeModel();
		assert nodeModel != null;

		// be aware of a possibly not executed nodeModel! The data you retrieve
		// from your nodemodel could be null, emtpy, or invalid in any kind.

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void onClose() {

		// TODO things to do when closing the view
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void onOpen() {

		// TODO things to do when opening the view
	}

}
