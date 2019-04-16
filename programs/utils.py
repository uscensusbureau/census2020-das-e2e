import zipfile
import glob
import os


def ship_files2spark(spark, tempdir, allfiles=False):
    """
    Zips the files in das_decennial folder and indicated subfolders to have as submodules and ships to Spark
    as zip python file.
    Also can ship as a regular file (for when code looks for non-py files)
    :param allfiles: whether to ship all files (as opposed to only python (.py) files)
    :param tempdir: directory for zipfile creation
    :param spark: SparkSession where to ship the files
    :return:
    """
    # das_decennial directory
    ddecdir = os.path.dirname(os.path.dirname(__file__))
    zipf = zipfile.ZipFile(os.path.join(tempdir, 'submodules.zip'), 'w', zipfile.ZIP_DEFLATED)

    # Subdirectories to add
    subdirs = ['programs', 'das_framework']

    # File with extension to zip
    ext = '*' if allfiles else 'py'

    # Add the files to zip, keeping the directory hierarchy
    for submodule_dir in subdirs:
        files2ship = [fn for fn in glob.iglob(os.path.join(ddecdir, f'{submodule_dir}/**/*.{ext}'), recursive=True)]

        for fname in files2ship:
            zipf.write(os.path.join(ddecdir, fname),
                       arcname=submodule_dir.split('/')[-1] + fname.split(submodule_dir)[1])

    # Add files in the das_decennial directory
    for fullname in glob.glob(os.path.join(ddecdir, f'*.{ext}')):
        zipf.write(fullname, arcname=os.path.basename(fullname))

    zipf.close()

    spark.sparkContext.addPyFile(zipf.filename)

    if allfiles:
        spark.sparkContext.addFile(zipf.filename)
