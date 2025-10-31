from pyrogram import Client
from pyrogram.raw.types.auth.exported_authorization import ExportedAuthorization
from pyrogram.raw.functions.auth.export_authorization import ExportAuthorization
from pyrogram.raw.functions.auth.import_authorization import ImportAuthorization
from pyrogram.raw.base.input_file_location import InputFileLocation
from pyrogram.raw.types.input_document_file_location import InputDocumentFileLocation
from pyrogram.raw.types.upload.file import File
from pyrogram.raw.types.upload.file_cdn_redirect import FileCdnRedirect
from pyrogram.raw.functions.upload.get_file import GetFile
from pyrogram.raw.types.upload.cdn_file import CdnFile
from pyrogram.raw.types.upload.cdn_file_reupload_needed import CdnFileReuploadNeeded
from pyrogram.raw.functions.upload.get_cdn_file import GetCdnFile
from pyrogram.raw.functions.upload.reupload_cdn_file import ReuploadCdnFile
from pyrogram.raw.types.file_hash import FileHash
from pyrogram.raw.functions.upload.get_cdn_file_hashes import GetCdnFileHashes
from pyrogram.session import Auth, Session
from pyrogram.crypto import aes
from pyrogram.errors import CDNFileHashMismatch
from hashlib import sha256
from logging import Logger
from io import BytesIO

import typing


async def download_documents(
    client: Client,
    documents_data: dict[int, list[tuple[int, int, bytes]]],  # {dc_id: [(document_id, access_hash, file_reference), ...]}
    logger: Logger
) -> dict[int, BytesIO]:  # {document_id: BytesIO}
    downloaded_documents: dict[int, BytesIO] = {}
    total_documents_count = sum(len(documents) for documents in documents_data.values())

    for dc_id, documents in documents_data.items():
        session = Session(
            client,
            dc_id,
            (
                await Auth(
                    client,
                    dc_id,
                    typing.cast(bool, await client.storage.test_mode()),
                ).create()
                if dc_id != await client.storage.dc_id() else
                typing.cast(bytes, await client.storage.auth_key())
            ),
            typing.cast(bool, await client.storage.test_mode()),
            is_media = True
        )

        await session.start()

        if dc_id != await client.storage.dc_id():
            exported_auth = typing.cast(ExportedAuthorization, await client.invoke(
                ExportAuthorization(
                    dc_id = dc_id
                )
            ))

            await session.invoke(
                ImportAuthorization(
                    id = exported_auth.id,
                    bytes = typing.cast(bytes, exported_auth.bytes)
                )
            )

        for document_id, document_access_hash, document_file_reference in documents:
            file = BytesIO()

            offset_bytes = 0
            chunk_size = 1024 * 1024 # 1 MB

            location = typing.cast(InputFileLocation, InputDocumentFileLocation(
                id = document_id,
                access_hash = document_access_hash,
                file_reference = document_file_reference,
                thumb_size = ""  # For documents, this is typically empty
            ))

            r = typing.cast(
                File | FileCdnRedirect,
                await session.invoke(
                    GetFile(
                        location = location,
                        offset = offset_bytes,
                        limit = chunk_size
                    ),
                    sleep_threshold = client.sleep_threshold
                )
            )

            if isinstance(r, File):
                while True:
                    chunk = typing.cast(bytes, r.bytes)
                    file.write(chunk)
                    offset_bytes += chunk_size

                    if len(chunk) < chunk_size:
                        break

                    r = typing.cast(File, await session.invoke(
                        GetFile(
                            location = location,
                            offset = offset_bytes,
                            limit = chunk_size
                        ),
                        sleep_threshold = client.sleep_threshold
                    ))

            else:  # raw.types.upload.FileCdnRedirect
                cdn_session = Session(
                    client,
                    r.dc_id,
                    await Auth(
                        client,
                        r.dc_id,
                        typing.cast(bool, await client.storage.test_mode())
                    ).create(),
                    typing.cast(bool, await client.storage.test_mode()),
                    is_media = True,
                    is_cdn = True
                )

                try:
                    await cdn_session.start()

                    while True:
                        r2 = typing.cast(
                            CdnFile | CdnFileReuploadNeeded,
                            await cdn_session.invoke(
                                GetCdnFile(
                                    file_token = r.file_token,
                                    offset = offset_bytes,
                                    limit = chunk_size
                                )
                            )
                        )

                        if isinstance(r2, CdnFileReuploadNeeded):
                            await session.invoke(  # Reupload request goes to the original session, not CDN
                                ReuploadCdnFile(
                                    file_token = r.file_token,
                                    request_token = r2.request_token
                                )
                            )
                            continue

                        chunk = typing.cast(bytes, r2.bytes)

                        decrypted_chunk = aes.ctr256_decrypt(
                            chunk,
                            r.encryption_key,
                            bytearray(r.encryption_iv[:-4] + (offset_bytes // 16).to_bytes(4, "big"))
                        )

                        # Note: GetCdnFileHashes offset is not necessarily the same as file offset
                        # It's an offset within the CDN file, relative to a block start, usually.
                        # For simplicity, assuming the offset for hashes aligns with the current file offset
                        # Pyrogram's internal CDN handling is more robust here.
                        # This part might need careful testing and alignment with Pyrogram's source if issues arise.
                        hashes = typing.cast(list[FileHash], await session.invoke(
                            GetCdnFileHashes(
                                file_token = r.file_token,
                                offset = offset_bytes
                            )
                        ))

                        hash_offset = 0

                        for h in hashes:
                            cdn_chunk_to_hash = decrypted_chunk[hash_offset : hash_offset + h.limit]

                            CDNFileHashMismatch.check(
                                h.hash == sha256(cdn_chunk_to_hash).digest(),
                                "CDN file hash mismatch!"
                            )

                            hash_offset += h.limit

                        file.write(decrypted_chunk)
                        offset_bytes += chunk_size

                        if len(chunk) < chunk_size:
                            break

                finally:
                    await cdn_session.stop()

            downloaded_documents[document_id] = file

            logger.info(f"Downloaded {len(downloaded_documents)}/{total_documents_count} documents ({dc_id} | {document_id})")

        await session.stop()

    return downloaded_documents
