package de.pavloff.spark4knime.input;

import org.knime.core.node.NodeView;

import de.pavloff.spark4knime.TableCellUtils.RddViewer;

/**
 * <code>NodeView</code> for the "FileToRDD" Node. Creates view and show first
 * entries of an JavaRDD object.
 * 
 * Viewer is handled by NodeModel. Only during onOpen() the data will be taken
 * from RDD and showed.
 * 
 * @author Oleg Pavlov, University of Heidelberg
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
		// instantiate the components of the view here.
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void modelChanged() {
		// retrieve the new model from your nodemodel and update the view.
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
		// things to do when closing the view
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void onOpen() {
		// things to do when opening the view
		FileToRDDNodeModel nodeModel = (FileToRDDNodeModel) getNodeModel();
		assert nodeModel != null;
		RddViewer view = nodeModel.getRddViewer();
		assert (view != null);

		setComponent(view.getTableView());
	}

}
